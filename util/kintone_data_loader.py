import pandas as pd
from util.kintone_fetch import KintoneDataManager


def load_inventory_inputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    calc_monthly_inventory 用に必要な 3 つの DataFrame を取得する。

    戻り値:
      plchange_df, id_df, item_df
    """

    kdm = KintoneDataManager()

    # 必要なアプリだけフェッチ
    apps_to_fetch = ["plchange", "id", "item"]
    kdm.fetch_selected(apps_to_fetch)

    # KintoneDataManager 内の DataFrame を取得
    tables_to_fetch = [
        "plchange_main",
        "id_main",
        "item_df_sub_1",
    ]

    plchange_main, id_raw, item_raw = kdm.get_multiple_dataframes(*tables_to_fetch)

    # --- カラム整形 ---

    plchange_df = plchange_main.loc[:, ["識別子", "重量案分後PL換算係数"]].copy()

    id_df = id_raw.loc[
        :,
        [
            "識別子",
            "品名",
            "法令",
            "保管形態",
            "倉庫機能",
            "通常期_在庫維持月数",
            "備蓄期_在庫維持月数",
        ],
    ].copy()

    # ★ 月は文字列のまま（datetime 変換しない）
    item_df = item_raw.loc[:, ["品名", "月", "繁忙度"]].copy()

    print("plchange_df, id_df, item_df が取得されました。")

    return plchange_df, id_df, item_df

def load_optimization_masters() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    在庫最適化に必要なマスタを取得する。

    戻り値:
      place_master       : 倉庫（場所）の構造情報
      warehouse_master   : 置場ごとの容量
      id_warehouse_master: SKU×置場の適合情報
      cost_master        : 在庫配置で使う費用情報（保管＋入出庫＋固定費）
    """
    kdm = KintoneDataManager()

    apps_to_fetch = ["place", "warehouse", "id_warehouse"]
    kdm.fetch_selected(apps_to_fetch)

    tables_to_fetch = [
        "place_df_sub_1",
        "warehouse_main",
        "id_warehouse_main",
    ]

    place_raw, warehouse_raw, id_warehouse_raw = kdm.get_multiple_dataframes(*tables_to_fetch)

    # --- place: 構造マスタ ---
    place_master = place_raw.loc[
        :,
        ["場内外区分", "場所id", "場所名"],
    ].drop_duplicates().copy()

    # --- cost: 費用マスタ（まずは全部）---
    cost_master = place_raw.loc[
        :,
        ["場所id", "区分", "分類", "分類明細", "cost", "単位"],
    ].copy()

    # 数値変換（NaN → 0 に統一）
    cost_master["cost"] = (
        pd.to_numeric(cost_master["cost"], errors="coerce")
        .fillna(0)
    )

    # ★ 今回使うのは：
    #   ・区分 = "固定"
    #   ・分類 ∈ {"保管", "入出庫"}
    cost_master = cost_master[
        (cost_master["区分"] == "固定")
        | (cost_master["分類"].isin(["保管", "入出庫"]))
    ].copy()

    # --- warehouse: 置場マスタ（容量付き）---
    warehouse_master = warehouse_raw.loc[
        :,
        ["場所id", "置場id", "場所名", "置場名", "容量"],
    ].copy()
    warehouse_master = warehouse_master.rename(columns={"容量": "capacity_pl"})
    warehouse_master["capacity_pl"] = pd.to_numeric(
        warehouse_master["capacity_pl"], errors="coerce"
    )

    # --- id_warehouse: SKU×置場の適合マスタ ---
    id_warehouse_master = id_warehouse_raw.loc[
        :,
        ["ver_id", "識別子", "置場id", "置場区分", "区分説明", "出荷場所", "出荷場所名"],
    ].copy()

    print("place_master, warehouse_master, id_warehouse_master, cost_master が取得されました。")

    return place_master, warehouse_master, id_warehouse_master, cost_master
