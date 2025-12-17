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

# COMMAND ----------

# CSVパスを直接指定
demand_csv_path = "/Volumes/zone2_mo/chiba_warehouse/demand/20251208.csv"

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
# MAGIC ## 5. Unity Catalogへの保存

# COMMAND ----------

# PandasデータフレームをSparkデータフレームに変換
psi_spark_df = spark.createDataFrame(psi_df)
allocation_summary_spark_df = spark.createDataFrame(allocation_summary_df)

# Unity Catalogのテーブルに追記（append）
psi_spark_df.write.mode("append").saveAsTable("zone2_mo.chiba_warehouse.psi")
allocation_summary_spark_df.write.mode("append").saveAsTable("zone2_mo.chiba_warehouse.allocation_summary")

print("[INFO] データをUnity Catalogに保存しました")
print("  - zone2_mo.chiba_warehouse.psi")
print("  - zone2_mo.chiba_warehouse.allocation_summary")
