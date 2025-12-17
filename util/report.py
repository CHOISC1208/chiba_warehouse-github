import pandas as pd


def build_allocation_summary(
    allocation_df: pd.DataFrame,
    psi_df: pd.DataFrame,
    warehouse_master: pd.DataFrame,
    id_warehouse_master: pd.DataFrame,
    cost_master: pd.DataFrame,
) -> pd.DataFrame:
    """
    最適化結果から取引タイプごとの詳細レポートを生成する。

    取引タイプ:
      - 保管: EndInv_pl × 保管単価
      - 入庫: In_pl × 入出庫単価
      - 出庫: Sales_pl × 入出庫単価
      - 倉庫間出庫: Sales_pl × 元倉庫の入出庫単価（区分1のみ）
      - 倉庫間入庫: Sales_pl × 出荷場所の入出庫単価（区分1のみ）

    Args:
        allocation_df: 最適化結果（識別子, 年月, 置場id, x_pl, ...）
        psi_df: PSI情報（識別子, 年月, BeginInv_pl, In_pl, Sales_pl, EndInv_pl）
        warehouse_master: 倉庫マスタ（場所id, 置場id, 場所名, 置場名）
        id_warehouse_master: SKU-倉庫関係（識別子, 置場id, 置場区分, 出荷場所, 出荷場所名）
        cost_master: コストマスタ（場所id, 区分, 分類, cost, 単位）

    Returns:
        transaction_df: 取引明細
          - 識別子
          - 年月
          - 取引タイプ
          - 置場id
          - 置場名
          - 移動先置場id（倉庫間移動の場合のみ）
          - 移動先置場名（倉庫間移動の場合のみ）
          - 数量(pl)
          - 単価
          - コスト
    """

    if allocation_df.empty:
        print("[WARNING] allocation_df is empty!")
        return pd.DataFrame(
            columns=[
                "識別子", "年月", "取引タイプ", "置場id", "置場名",
                "移動先置場id", "移動先置場名", "数量(pl)", "単価", "コスト"
            ]
        )

    # 1) 必要な情報を結合
    # 年月フォーマットを統一（日付形式に変換）
    df = allocation_df.copy()
    df["年月"] = pd.to_datetime(df["年月"]).dt.normalize()

    psi_merge = psi_df[["識別子", "年月", "BeginInv_pl", "In_pl", "Sales_pl", "EndInv_pl"]].copy()
    psi_merge["年月"] = pd.to_datetime(psi_merge["年月"]).dt.normalize()

    # allocation_df に PSI情報を結合
    df = df.merge(
        psi_merge,
        on=["識別子", "年月"],
        how="left"
    )

    # 倉庫別PSI計算のため、時系列順にソート
    df = df.sort_values(["識別子", "置場id", "年月"]).reset_index(drop=True)

    # 前月EndInvを取得（前月のx_plが今月のBeginInv_倉庫になる）
    df["前月年月"] = df["年月"] - pd.DateOffset(months=1)
    prev_inv = df[["識別子", "置場id", "年月", "x_pl"]].copy()
    prev_inv = prev_inv.rename(columns={"年月": "次月年月", "x_pl": "前月EndInv_倉庫"})

    df = df.merge(
        prev_inv,
        left_on=["識別子", "置場id", "前月年月"],
        right_on=["識別子", "置場id", "次月年月"],
        how="left"
    )
    df["前月EndInv_倉庫"] = df["前月EndInv_倉庫"].fillna(0)
    df = df.drop(columns=["前月年月", "次月年月"], errors="ignore")

    # データ開始月を特定（各識別子の最初の年月）
    first_months = df.groupby("識別子")["年月"].min().to_dict()
    df["is_first_month"] = df.apply(lambda row: row["年月"] == first_months.get(row["識別子"]), axis=1)

    # 識別子×年月ごとのBeginInv_plを集計（按分の基準として使用）
    # 既に前月EndInv_倉庫の合計が BeginInv_pl になるはずだが、念のため確認用
    df_grouped_beginv = df.groupby(["識別子", "年月"])["前月EndInv_倉庫"].sum().reset_index()
    df_grouped_beginv = df_grouped_beginv.rename(columns={"前月EndInv_倉庫": "実際のBeginInv_pl"})
    df = df.merge(df_grouped_beginv, on=["識別子", "年月"], how="left")

    # warehouse_master から場所id, 置場名を取得
    wh_info = warehouse_master[["置場id", "場所id", "場所名", "置場名"]].drop_duplicates()
    df = df.merge(wh_info, on="置場id", how="left")

    # id_warehouse_master から置場区分, 出荷場所を取得
    # 重要: 識別子×置場idの組み合わせで重複がないように確実に1行にする
    id_wh_info = id_warehouse_master[
        ["識別子", "置場id", "置場区分", "出荷場所", "出荷場所名"]
    ].drop_duplicates(subset=["識別子", "置場id"], keep="first")

    df = df.merge(id_wh_info, on=["識別子", "置場id"], how="left")

    # 2) コスト単価の取得（場所idごとに保管費、入出庫費を取得）
    # 保管費
    storage_cost = cost_master[
        (cost_master["分類"] == "保管") &
        (cost_master["単位"].isin(["PL", "円/PL"]))
    ][["場所id", "cost"]].drop_duplicates(subset=["場所id"], keep="first")
    storage_cost = storage_cost.rename(columns={"cost": "保管単価"})

    # 入出庫費
    io_cost = cost_master[
        (cost_master["分類"] == "入出庫") &
        (cost_master["単位"].isin(["PL", "円/PL"]))
    ][["場所id", "cost"]].drop_duplicates(subset=["場所id"], keep="first")
    io_cost = io_cost.rename(columns={"cost": "入出庫単価"})

    # 場所idごとの単価をマージ
    df = df.merge(storage_cost, on="場所id", how="left")
    df = df.merge(io_cost, on="場所id", how="left")

    # 出荷場所の入出庫単価も取得（区分1の倉庫間移動用）
    # 出荷場所の場所idを取得
    ship_wh = warehouse_master[["置場id", "場所id"]].drop_duplicates(subset=["置場id"], keep="first")
    ship_wh = ship_wh.rename(columns={"置場id": "出荷場所", "場所id": "出荷場所_場所id"})
    df = df.merge(ship_wh, on="出荷場所", how="left")

    # 出荷場所の入出庫単価
    ship_io_cost = io_cost.rename(columns={"場所id": "出荷場所_場所id", "入出庫単価": "出荷場所_入出庫単価"})
    df = df.merge(ship_io_cost, on="出荷場所_場所id", how="left")

    # 3) トランザクション生成
    transactions = []

    for idx, row in df.iterrows():
        識別子 = row["識別子"]
        年月 = row["年月"]
        置場id = row["置場id"]
        置場名 = row["置場名"]
        置場区分 = row["置場区分"]

        # allocation_dfのx_plは、この倉庫に配置された在庫量
        EndInv_倉庫 = row["x_pl"]
        前月EndInv_倉庫 = row["前月EndInv_倉庫"]
        is_first_month = row["is_first_month"]

        # PSI情報（識別子×年月の合計値）
        BeginInv_pl = row["BeginInv_pl"]
        In_pl = row["In_pl"]
        Sales_pl = row["Sales_pl"]
        EndInv_pl = row["EndInv_pl"]
        実際のBeginInv_pl = row["実際のBeginInv_pl"]

        保管単価 = row.get("保管単価", 0)
        入出庫単価 = row.get("入出庫単価", 0)

        # PSI情報がNaNの場合はスキップ
        if pd.isna(EndInv_pl) or pd.isna(In_pl) or pd.isna(Sales_pl):
            continue

        # 置場区分を整数に変換（文字列の場合もある）
        try:
            置場区分 = int(置場区分)
        except (ValueError, TypeError):
            continue

        # 倉庫別PSI計算
        倉庫保管量 = EndInv_倉庫

        if is_first_month:
            # データ開始月の場合：BeginInv_plとIn_plを按分
            if EndInv_pl > 0:
                BeginInv_倉庫 = BeginInv_pl * (EndInv_倉庫 / EndInv_pl)
                倉庫入庫量 = In_pl * (EndInv_倉庫 / EndInv_pl)
            else:
                BeginInv_倉庫 = 0
                倉庫入庫量 = 0
            # Sales_倉庫は逆算: BeginInv + In - Sales = EndInv
            倉庫出庫量 = BeginInv_倉庫 + 倉庫入庫量 - EndInv_倉庫
            # 負の値は0に補正
            倉庫出庫量 = max(0, 倉庫出庫量)

        elif 前月EndInv_倉庫 == 0 and EndInv_倉庫 > 0:
            # 新規倉庫の場合：全量入庫
            BeginInv_倉庫 = 0
            倉庫入庫量 = EndInv_倉庫
            倉庫出庫量 = 0

        else:
            # 既存倉庫の場合
            BeginInv_倉庫 = 前月EndInv_倉庫

            # Sales_plを前月在庫（BeginInv）の比率で按分
            if 実際のBeginInv_pl > 0:
                倉庫出庫量 = Sales_pl * (BeginInv_倉庫 / 実際のBeginInv_pl)
            else:
                倉庫出庫量 = 0

            # In_倉庫は逆算: BeginInv + In - Sales = EndInv
            倉庫入庫量 = EndInv_倉庫 + 倉庫出庫量 - BeginInv_倉庫
            # 負の値は0に補正
            倉庫入庫量 = max(0, 倉庫入庫量)

        # 区分2（保管&出荷可能）の場合
        if 置場区分 == 2:
            # 1. 保管
            transactions.append({
                "識別子": 識別子,
                "年月": 年月,
                "取引タイプ": "保管",
                "置場id": 置場id,
                "置場名": 置場名,
                "移動先置場id": None,
                "移動先置場名": None,
                "数量(pl)": 倉庫保管量,
                "単価": 保管単価,
                "コスト": 倉庫保管量 * 保管単価,
            })

            # 2. 入庫
            transactions.append({
                "識別子": 識別子,
                "年月": 年月,
                "取引タイプ": "入庫",
                "置場id": 置場id,
                "置場名": 置場名,
                "移動先置場id": None,
                "移動先置場名": None,
                "数量(pl)": 倉庫入庫量,
                "単価": 入出庫単価,
                "コスト": 倉庫入庫量 * 入出庫単価,
            })

            # 3. 出庫
            transactions.append({
                "識別子": 識別子,
                "年月": 年月,
                "取引タイプ": "出庫",
                "置場id": 置場id,
                "置場名": 置場名,
                "移動先置場id": None,
                "移動先置場名": None,
                "数量(pl)": 倉庫出庫量,
                "単価": 入出庫単価,
                "コスト": 倉庫出庫量 * 入出庫単価,
            })

        # 区分1（保管専用）の場合
        elif 置場区分 == 1:
            出荷場所 = row["出荷場所"]
            出荷場所名 = row["出荷場所名"]
            出荷場所_入出庫単価 = row.get("出荷場所_入出庫単価", 0)

            # 1. 保管
            transactions.append({
                "識別子": 識別子,
                "年月": 年月,
                "取引タイプ": "保管",
                "置場id": 置場id,
                "置場名": 置場名,
                "移動先置場id": None,
                "移動先置場名": None,
                "数量(pl)": 倉庫保管量,
                "単価": 保管単価,
                "コスト": 倉庫保管量 * 保管単価,
            })

            # 2. 入庫
            transactions.append({
                "識別子": 識別子,
                "年月": 年月,
                "取引タイプ": "入庫",
                "置場id": 置場id,
                "置場名": 置場名,
                "移動先置場id": None,
                "移動先置場名": None,
                "数量(pl)": 倉庫入庫量,
                "単価": 入出庫単価,
                "コスト": 倉庫入庫量 * 入出庫単価,
            })

            # 3. 倉庫間出庫（元倉庫から出荷場所への移動）
            transactions.append({
                "識別子": 識別子,
                "年月": 年月,
                "取引タイプ": "倉庫間出庫",
                "置場id": 置場id,
                "置場名": 置場名,
                "移動先置場id": 出荷場所,
                "移動先置場名": 出荷場所名,
                "数量(pl)": 倉庫出庫量,
                "単価": 入出庫単価,
                "コスト": 倉庫出庫量 * 入出庫単価,
            })

            # 4. 倉庫間入庫（出荷場所での受け入れ）
            transactions.append({
                "識別子": 識別子,
                "年月": 年月,
                "取引タイプ": "倉庫間入庫",
                "置場id": 出荷場所,
                "置場名": 出荷場所名,
                "移動先置場id": 置場id,
                "移動先置場名": 置場名,
                "数量(pl)": 倉庫出庫量,
                "単価": 出荷場所_入出庫単価,
                "コスト": 倉庫出庫量 * 出荷場所_入出庫単価,
            })

            # 5. 出庫（出荷場所から顧客への出荷）
            transactions.append({
                "識別子": 識別子,
                "年月": 年月,
                "取引タイプ": "出庫",
                "置場id": 出荷場所,
                "置場名": 出荷場所名,
                "移動先置場id": None,
                "移動先置場名": None,
                "数量(pl)": 倉庫出庫量,
                "単価": 出荷場所_入出庫単価,
                "コスト": 倉庫出庫量 * 出荷場所_入出庫単価,
            })

    transaction_df = pd.DataFrame(transactions)
    return transaction_df


def build_warehouse_utilization(
    allocation_df: pd.DataFrame,
    warehouse_master: pd.DataFrame,
) -> pd.DataFrame:
    """
    allocation_df と warehouse_master から
    倉庫（置場）×月の充填率を計算する。

    戻り値:
      utilization_df:
        - 置場id
        - 場所名
        - 置場名
        - 年月
        - total_pl      : その月の在庫合計（PL）
        - capacity_pl   : 容量（PL）
        - utilization   : 充填率（0〜1）
    """

    if allocation_df.empty:
        return pd.DataFrame(
            columns=[
                "置場id",
                "場所名",
                "置場名",
                "年月",
                "total_pl",
                "capacity_pl",
                "utilization",
            ]
        )

    # 1) 倉庫×月で合計PL
    usage = (
        allocation_df
        .groupby(["置場id", "年月"], as_index=False)["x_pl"]
        .sum()
        .rename(columns={"x_pl": "total_pl"})
    )

    # 2) 容量と名称をマージ
    wh = warehouse_master.loc[
        :, ["置場id", "場所名", "置場名", "capacity_pl"]
    ].copy()

    utilization_df = usage.merge(wh, on="置場id", how="left")

    # 3) 充填率計算（0除算はNaNになるので、そのまま or 0埋めなど運用で決める）
    utilization_df["capacity_pl"] = pd.to_numeric(
        utilization_df["capacity_pl"], errors="coerce"
    )
    utilization_df["utilization"] = (
        utilization_df["total_pl"] / utilization_df["capacity_pl"]
    )

    return utilization_df


def build_cost_summary(
    allocation_df: pd.DataFrame,
    id_warehouse_master: pd.DataFrame,
) -> pd.DataFrame:
    """
    allocation_df + id_warehouse_master から、
    倉庫×年月×識別子×倉庫区分ごとのコストサマリを作る。

    出力カラム:
      - 置場id        : 倉庫id
      - 年月
      - 識別子
      - 置場区分      : 1 or 2
      - total_pl
      - storage_cost_total
      - handling_cost_total  (入出庫コスト)
      - total_cost
    """
    if allocation_df.empty:
        return pd.DataFrame(
            columns=[
                "置場id",
                "年月",
                "識別子",
                "置場区分",
                "total_pl",
                "storage_cost_total",
                "handling_cost_total",
                "total_cost",
            ]
        )

    # 倉庫区分をマージ（識別子×置場id で紐づけ）
    id_wh = id_warehouse_master.loc[
        :, ["識別子", "置場id", "置場区分"]
    ].drop_duplicates()

    df = allocation_df.merge(
        id_wh,
        on=["識別子", "置場id"],
        how="left",
    )

    # まとめ
    cost_summary_df = (
        df.groupby(
            ["置場id", "年月", "識別子", "置場区分"],
            as_index=False,
        )
        .agg(
            total_pl=("x_pl", "sum"),
            storage_cost_total=("storage_cost_total", "sum"),
            handling_cost_total=("handling_cost_total", "sum"),
            total_cost=("total_cost", "sum"),
        )
    )

    return cost_summary_df
