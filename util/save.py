import pandas as pd
import os
from datetime import datetime


OUTPUT_DIR = "data/optimize"

def save_results(
    psi_df: pd.DataFrame,
    allocation_df: pd.DataFrame,
    cost_summary_df: pd.DataFrame,
    utilization_df: pd.DataFrame,
    output_dir: str = OUTPUT_DIR,
) -> None:
    """最適化結果と PSI を CSV に保存するヘルパー関数。"""

    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")

    psi_path = os.path.join(output_dir, f"psi_{timestamp}.csv")
    allocation_path = os.path.join(output_dir, f"allocation_{timestamp}.csv")
    cost_summary_path = os.path.join(output_dir, f"cost_summary_{timestamp}.csv")
    utilization_path = os.path.join(output_dir, f"utilization_{timestamp}.csv")

    psi_df.to_csv(psi_path, index=False, encoding="utf-8-sig")
    allocation_df.to_csv(allocation_path, index=False, encoding="utf-8-sig")
    cost_summary_df.to_csv(cost_summary_path, index=False, encoding="utf-8-sig")
    utilization_df.to_csv(utilization_path, index=False, encoding="utf-8-sig")

    print("[INFO] 結果を保存しました:")
    print(f"  - psi:          {psi_path}")
    print(f"  - allocation:   {allocation_path}")
    print(f"  - cost_summary: {cost_summary_path}")
    print(f"  - utilization:  {utilization_path}")