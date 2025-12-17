import pandas as pd
import os
from datetime import datetime


OUTPUT_DIR = "data/optimize"

def save_results(
    psi_df: pd.DataFrame,
    allocation_summary_df: pd.DataFrame,
    output_dir: str = OUTPUT_DIR,
) -> None:
    """
    最適化結果と PSI を CSV に保存するヘルパー関数。

    Args:
        psi_df: PSI情報（識別子×年月のBeginInv, In, Sales, EndInv）
        allocation_summary_df: 取引タイプごとの詳細レポート
        output_dir: 出力ディレクトリ（デフォルト: data/optimize）
    """

    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")

    psi_path = os.path.join(output_dir, f"psi_{timestamp}.csv")
    allocation_summary_path = os.path.join(output_dir, f"allocation_summary_{timestamp}.csv")

    psi_df.to_csv(psi_path, index=False, encoding="utf-8-sig")
    allocation_summary_df.to_csv(allocation_summary_path, index=False, encoding="utf-8-sig")

    print("[INFO] 結果を保存しました:")
    print(f"  - PSI:                {psi_path}")
    print(f"  - Allocation Summary: {allocation_summary_path}")