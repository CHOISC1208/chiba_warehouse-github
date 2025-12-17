import pandas as pd

from util.monthly_inventory import calc_monthly_inventory
from util.kintone_data_loader import (
    load_inventory_inputs,
    load_optimization_masters,
)
from util.psimake import build_sku_psi
from util.optimizer import optimize_inventory_allocation
from util.report import build_warehouse_utilization, build_cost_summary
from util.save import save_results


DEMAND_CSV_PATH = "data/20251208.csv"

def main() -> None:
    # ===== 1) 需要データ読み込み =====
    annual_sales_volume_df = pd.read_csv(DEMAND_CSV_PATH)
    print(f"[INFO] 需要CSVを読み込みました: {DEMAND_CSV_PATH}")

    # ===== 2) 在庫換算用マスタの取得 =====
    plchange_df, id_df, item_df = load_inventory_inputs()

    # 需要 → 在庫換算（t → PL、在庫維持月数を反映）
    rawpsi_df = calc_monthly_inventory(
        annual_sales_volume_df=annual_sales_volume_df,
        id_df=id_df,
        item_df=item_df,
        plchange_df=plchange_df,
    )

    # PSI整形（BeginInv_pl, In_pl, Sales_pl, EndInv_pl などを付与）
    psi_df = build_sku_psi(rawpsi_df)

    # ===== 3) 最適化用マスタの取得 =====
    place_master, warehouse_master, id_warehouse_master, cost_master = (
        load_optimization_masters()
    )

    # ===== 4) 在庫配置最適化 =====
    allocation_df, _ = optimize_inventory_allocation(
        psi_df=psi_df,
        warehouse_master=warehouse_master,
        id_warehouse_master=id_warehouse_master,
        cost_master=cost_master,
    )

    # 倉庫×識別子×区分ごとのコストサマリ
    cost_summary_df = build_cost_summary(
        allocation_df=allocation_df,
        id_warehouse_master=id_warehouse_master,
    )

    # 倉庫充填率
    utilization_df = build_warehouse_utilization(
        allocation_df=allocation_df,
        warehouse_master=warehouse_master,
    )

    # ===== 5) 結果保存 =====
    save_results(
        psi_df=psi_df,
        allocation_df=allocation_df,
        cost_summary_df=cost_summary_df,
        utilization_df=utilization_df,
    )


if __name__ == "__main__":
    main()
