# util/optimizer.py

from __future__ import annotations

from typing import Tuple, Dict

import pandas as pd
import pulp


def optimize_inventory_allocation(
    psi_df: pd.DataFrame,
    warehouse_master: pd.DataFrame,
    id_warehouse_master: pd.DataFrame,
    cost_master: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    PSI (psi_df) と 倉庫系マスタを使って、
    SKU×月×置場 の在庫配置を最適化する。

    前提:
      psi_df:
        - 識別子
        - 年月
        - EndInv_pl      : 必要在庫量（PL）
        （他の列があってもOK）

      warehouse_master:
        - 場所id
        - 置場id
        - capacity_pl    : 置場ごとの最大容量（PL）

      id_warehouse_master:
        - 識別子
        - 置場id
        - 置場区分       : 1=保管+出荷可, 2=保管のみ
        - 出荷場所       : 区分2のとき、出荷に使う置場id（区分1）
        （他の列があってもOK）

      cost_master:
        - 場所id
        - 区分
        - 分類           : "保管" or "入出庫" など
        - cost           : 数値（NaN→0 済み想定）
        - 単位           : "PL", "月" など

    戻り値:
      allocation_df:
        - 識別子
        - 年月
        - 置場id
        - x_pl          : 最適配置PL
        - storage_cost  : その置場の1PLあたり保管費
        - effective_io_cost : そのSKU×置場の1PLあたりIO費
        - total_cost    : x_pl × (storage_cost + effective_io_cost)

      cost_summary_df:
        - 集計用のサマリ（置場別 or 月別などはここで自由に集計）
    """

    # ===== 0) 前処理・キー準備 =====
    # I[i,t] = EndInv_pl
    psi = psi_df.copy()
    psi = psi[psi["EndInv_pl"] > 0].copy()

    # 年月は文字列に統一しておく（PuLPのキーに使いやすくするため）
    psi["年月"] = psi["年月"].astype(str)

    sku_list = sorted(psi["識別子"].unique().tolist())
    time_list = sorted(psi["年月"].unique().tolist())

    # I[(i,t)] 辞書化
    I: Dict[tuple, float] = (
        psi.set_index(["識別子", "年月"])["EndInv_pl"].to_dict()
    )

    # ===== 1) コスト (storage_cost[w], io_cost[w]) の作成 =====

    # 1-1. 保管コスト（場所id単位）
    storage_by_place = (
        cost_master[cost_master["分類"] == "保管"]
        .groupby("場所id")["cost"]
        .sum()
    )

    # 1-2. 入出庫コスト（場所id単位）
    io_by_place = (
        cost_master[cost_master["分類"] == "入出庫"]
        .groupby("場所id")["cost"]
        .sum()
    )

    # 1-3. 置場idへのマッピング
    wh = warehouse_master.copy()
    wh = wh[["場所id", "置場id", "capacity_pl"]].copy()

    wh["storage_cost"] = wh["場所id"].map(storage_by_place).fillna(0.0)
    wh["io_cost_base"] = wh["場所id"].map(io_by_place).fillna(0.0)

    # NaN容量は 0 とみなして保管不可扱いにする
    wh["capacity_pl"] = pd.to_numeric(wh["capacity_pl"], errors="coerce").fillna(0.0)

    # ===== 2) allowed[i,w] と effective_io_cost[i,w] の構築 =====

    # id_warehouse に origin / ship の場所idを付与
    id_wh = id_warehouse_master.copy()

    # origin（置場id→場所id）
    origin = wh[["置場id", "場所id"]].rename(
        columns={"場所id": "origin_placeid"}
    )
    id_wh = id_wh.merge(origin, on="置場id", how="left")

    # ship（出荷場所 = 置場id想定 → 場所id）
    ship = wh[["置場id", "場所id"]].rename(
        columns={"置場id": "出荷場所", "場所id": "ship_placeid"}
    )
    id_wh = id_wh.merge(ship, on="出荷場所", how="left")

    # base io_cost（場所id→単価）
    io_cost_place = io_by_place.to_dict()

    allowed = {}             # allowed[(i,w)] = True
    eff_io_cost = {}         # effective_io_cost[(i,w)] = 単価
    storage_cost = {}        # storage_cost[w] = 単価
    capacity = {}            # capacity[w] = 容量

    # 置場ごとの保管費・容量
    for _, row in wh.iterrows():
        w = row["置場id"]
        storage_cost[w] = float(row["storage_cost"])
        capacity[w] = float(row["capacity_pl"])

    # SKU×置場の許容・IOコスト
    for _, row in id_wh.iterrows():
        i = row["識別子"]
        w = row["置場id"]
        zone_type = row.get("置場区分", 1)  # 1 or 2想定

        origin_placeid = row.get("origin_placeid")
        ship_placeid = row.get("ship_placeid")

        # 基本は「この組み合わせは allowed」とみなす
        allowed[(i, w)] = True

        # origin / ship の IO 単価
        io_origin = io_cost_place.get(origin_placeid, 0.0)
        io_ship = io_cost_place.get(ship_placeid, 0.0)

        if zone_type == 1:
            # 区分1：出荷可能 → origin の IO だけを考慮
            eff = io_origin
        else:
            # 区分2：保管のみ → 元倉庫出庫 + 出荷倉庫入出庫 の2箇所ぶん
            eff = io_origin + io_ship

        eff_io_cost[(i, w)] = float(eff)

    # ===== 3) 最適化モデル構築 =====

    model = pulp.LpProblem("Inventory_Location_Optimization", pulp.LpMinimize)

    # 3-1. 変数 x[i,t,w] >= 0
    x = {}
    for i in sku_list:
        # i が PSIに存在しないタイミングはスキップ
        times_for_i = psi.loc[psi["識別子"] == i, "年月"].unique().tolist()

        # i がどの置場に allowed か
        ws_for_i = [w for (ii, w) in allowed.keys() if ii == i]

        for t in times_for_i:
            I_it = I.get((i, t), 0.0)
            if I_it <= 0:
                continue

            for w in ws_for_i:
                # capacity[w] == 0 の置場は変数を作らない
                if capacity.get(w, 0.0) <= 0:
                    continue

                var_name = f"x_{i}_{t}_{w}"
                x[(i, t, w)] = pulp.LpVariable(var_name, lowBound=0, cat="Continuous")

    # 3-2. 目的関数
    obj_terms = []
    for (i, t, w), var in x.items():
        sc = storage_cost.get(w, 0.0)
        io_eff = eff_io_cost.get((i, w), 0.0)
        unit_cost = sc + io_eff
        obj_terms.append(unit_cost * var)

    model += pulp.lpSum(obj_terms), "TotalCost"

    # 3-3. 制約：SKU×月の必要在庫量
    for (i, t), I_it in I.items():
        if I_it <= 0:
            continue

        vars_it = [var for (ii, tt, ww), var in x.items() if ii == i and tt == t]
        if not vars_it:
            # 配置先が一つもない場合はスキップ（あるいは例外にしてもよい）
            continue

        model += (
            pulp.lpSum(vars_it) == I_it,
            f"InvBalance_{i}_{t}",
        )

    # 3-4. 制約：倉庫容量
    for w in wh["置場id"].unique():
        cap = capacity.get(w, 0.0)
        if cap <= 0:
            continue

        vars_wt = {}
        for (i, t, ww), var in x.items():
            if ww == w:
                vars_wt.setdefault(t, []).append(var)

        for t, vars_list in vars_wt.items():
            model += (
                pulp.lpSum(vars_list) <= cap,
                f"Capacity_{w}_{t}",
            )

    # ===== 4) 求解 =====
    model.solve(pulp.PULP_CBC_CMD(msg=False))

    print("Solver status:", pulp.LpStatus[model.status])

    # ===== 5) 結果 DataFrame 化 =====
    records = []
    for (i, t, w), var in x.items():
        val = var.value()
        if val is None or val <= 0:
            continue

        sc = storage_cost.get(w, 0.0)
        io_eff = eff_io_cost.get((i, w), 0.0)

        storage_cost_total = val * sc
        handling_cost_total = val * io_eff
        total_cost = storage_cost_total + handling_cost_total

        records.append(
            {
                "識別子": i,
                "年月": t,
                "置場id": w,
                "x_pl": val,                       # 在庫配置PL
                "storage_cost": sc,                # 1PLあたり保管費
                "effective_io_cost": io_eff,       # 1PLあたり入出庫費（origin+shipを含む）
                "storage_cost_total": storage_cost_total,
                "handling_cost_total": handling_cost_total,
                "total_cost": total_cost,
            }
        )

    allocation_df = pd.DataFrame(records)


    # サマリ例：置場×月での合計PLとコスト
    if not allocation_df.empty:
        cost_summary_df = (
            allocation_df.groupby(["置場id", "年月"])
            .agg(
                total_pl=("x_pl", "sum"),
                storage_cost_sum=("storage_cost", "sum"),  # 単純和なので必要なら見直し
                total_cost=("total_cost", "sum"),
            )
            .reset_index()
        )
    else:
        cost_summary_df = pd.DataFrame(
            columns=["置場id", "年月", "total_pl", "storage_cost_sum", "total_cost"]
        )

    return allocation_df, cost_summary_df
