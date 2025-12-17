import pandas as pd


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
