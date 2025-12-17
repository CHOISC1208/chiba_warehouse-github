def build_sku_psi(df):
    """
    df: calc_monthly_inventory の出力（識別子×年月）
        必須列：
          - 販売数量 (t)
          - 重量案分後PL換算係数
          - 在庫数量(pl)
          - 識別子, 年月
    """

    df = df.sort_values(["識別子", "年月"]).copy()

    # ① 販売数量を PL に換算
    df["Sales_pl"] = df["販売数量"] * df["重量案分後PL換算係数"]

    # ② 期末在庫
    df["EndInv_pl"] = df["在庫数量(pl)"]

    # ③ 期首在庫（shift で前月の期末在庫を参照）
    df["BeginInv_pl"] = df.groupby("識別子")["EndInv_pl"].shift(1)

    # 初月は "定常" とみなして、Begin = End とする
    mask_first = df["BeginInv_pl"].isna()
    df.loc[mask_first, "BeginInv_pl"] = df.loc[mask_first, "EndInv_pl"]

    # ④ 受入数量（PL）
    df["In_pl"] = df["Sales_pl"] + df["EndInv_pl"] - df["BeginInv_pl"]

    # ⑤ 列の並び替え（BeginInv_pl, In_pl, Sales_pl, EndInv_pl の順に）
    cols = list(df.columns)

    # いったん対象4列を外す
    for c in ["Sales_pl", "EndInv_pl", "BeginInv_pl", "In_pl"]:
        cols.remove(c)

    # 先頭〜その他の列 ＋ 並べたい4列 の順で再構成
    df = df[cols + ["BeginInv_pl", "In_pl", "Sales_pl", "EndInv_pl"]]

    return df
