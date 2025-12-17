# Databricks notebook source
# MAGIC %md
# MAGIC # 在庫配置最適化ノートブック
# MAGIC
# MAGIC ## 使い方
# MAGIC 1. Databricks Secretsに以下を設定:
# MAGIC    - Scope: `kintone`
# MAGIC    - Key: `KINTONE_BASE_URL`
# MAGIC    - Value: `https://xxxx.cybozu.com/k/v1/records.json`
# MAGIC
# MAGIC 2. 需要データCSVをDBFSにアップロード:
# MAGIC    - `/dbfs/FileStore/warehouse_optimizer/input/20251208.csv`
# MAGIC
# MAGIC 3. このノートブックを実行

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. セットアップ

# COMMAND ----------

# 必要なライブラリのインストール
%pip install pulp python-dotenv

# COMMAND ----------

# ライブラリのインポート
import sys
sys.path.append("/Workspace/Repos/<your-username>/chiba_warehouse-github")

from main import run_optimization

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 需要データの準備
# MAGIC
# MAGIC 需要データを読み込む方法は3つあります：
# MAGIC - **方法A**: DBFSにアップロードしたCSVファイル
# MAGIC - **方法B**: Unity Catalogのテーブル
# MAGIC - **方法C**: 外部ストレージ（S3, Azure Blob等）

# COMMAND ----------

# 【方法A】DBFSにアップロードしたCSVファイルを使用
# ウィジェットでパスを指定
dbutils.widgets.text("csv_path", "/dbfs/FileStore/warehouse_optimizer/input/20251208.csv", "需要CSVパス")
demand_csv_path = dbutils.widgets.get("csv_path")

# または、直接指定
# demand_csv_path = "/dbfs/FileStore/warehouse_optimizer/input/20251208.csv"

# COMMAND ----------

# 【方法B】Unity Catalogのテーブルから読み込んでCSVに変換
# カタログからSparkデータフレームとして読み込み
# demand_df = spark.table("catalog_name.schema_name.demand_table")
#
# # 一時的にCSVとして保存
# temp_csv_path = "/dbfs/tmp/demand_data.csv"
# demand_df.toPandas().to_csv(temp_csv_path, index=False)
# demand_csv_path = temp_csv_path

# COMMAND ----------

# 【方法C】外部ストレージから読み込み
# Azure Blob Storageの例
# demand_csv_path = "wasbs://container@storageaccount.blob.core.windows.net/path/to/demand.csv"
#
# S3の例
# demand_csv_path = "s3://bucket-name/path/to/demand.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 最適化実行

# COMMAND ----------

# 最適化を実行
psi_df, allocation_summary_df = run_optimization(demand_csv_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. 結果の確認

# COMMAND ----------

# PSIデータの確認
display(psi_df)

# COMMAND ----------

# 取引明細データの確認
display(allocation_summary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. 結果の保存（オプション）
# MAGIC
# MAGIC DeltaテーブルやParquetファイルとして保存できます。

# COMMAND ----------

# Deltaテーブルとして保存
psi_df.write.format("delta").mode("append").saveAsTable("warehouse_optimization.psi")
allocation_summary_df.write.format("delta").mode("append").saveAsTable("warehouse_optimization.allocation_summary")

# COMMAND ----------

# または、Parquetファイルとして保存
output_path = "/dbfs/FileStore/warehouse_optimizer/output/"
psi_df.to_parquet(f"{output_path}psi.parquet")
allocation_summary_df.to_parquet(f"{output_path}allocation_summary.parquet")

# COMMAND ----------

# CSVとして保存（Excel用）
psi_df.to_csv(f"{output_path}psi.csv", index=False, encoding="utf-8-sig")
allocation_summary_df.to_csv(f"{output_path}allocation_summary.csv", index=False, encoding="utf-8-sig")

print(f"結果を {output_path} に保存しました")
