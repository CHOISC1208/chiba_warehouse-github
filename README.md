# 倉庫在庫配置最適化システム

## 概要

需要データと倉庫情報から、コスト最小化を目指した在庫配置を計算するシステムです。

## 機能

- 需要データ（t）→ 在庫数（PL）への換算
- PSI（Plan for every SKU Item）計算
- 倉庫別在庫配置の最適化（線形計画法）
- 倉庫別PSI計算（BeginInv + In - Sales = EndInv）
- 取引タイプ別コスト明細の出力
  - 保管、入庫、出庫、倉庫間出庫、倉庫間入庫

## 使い方

### Databricks（推奨）

Python環境不要で、ブラウザから実行できます。

詳細は [DATABRICKS_SETUP.md](./DATABRICKS_SETUP.md) を参照してください。

**クイックスタート:**

```python
# Databricks Notebook
%pip install pulp python-dotenv

import sys
sys.path.append("/Workspace/Repos/<your-username>/chiba_warehouse-github")
from main import run_optimization

# Unity Catalogから読み込む場合
demand_df = spark.table("catalog.schema.demand_table")
temp_csv_path = "/dbfs/tmp/demand_data.csv"
demand_df.toPandas().to_csv(temp_csv_path, index=False)

# 最適化実行
psi_df, allocation_summary_df = run_optimization(temp_csv_path)

# 結果表示
display(allocation_summary_df)
```

### ローカル実行

```bash
# 1. 依存関係のインストール
pip install -r requirements.txt

# 2. 環境変数の設定
echo "KINTONE_BASE_URL=https://xxxx.cybozu.com/k/v1/records.json" > .env

# 3. 需要データを配置
# data/20251208.csv

# 4. 実行
python main.py

# または、CSVパスを指定
python main.py /path/to/demand.csv
```

## 出力

### 1. PSI（psi_df）

| カラム | 説明 |
|--------|------|
| 識別子 | SKU識別子 |
| 年月 | 対象年月 |
| BeginInv_pl | 期首在庫（PL） |
| In_pl | 入庫量（PL） |
| Sales_pl | 出庫量（PL） |
| EndInv_pl | 期末在庫（PL） |
| created_at | 実行日時 |

### 2. 取引明細（allocation_summary_df）

| カラム | 説明 |
|--------|------|
| 識別子 | SKU識別子 |
| 年月 | 対象年月 |
| 取引タイプ | 保管/入庫/出庫/倉庫間出庫/倉庫間入庫 |
| 置場id | 倉庫ID |
| 置場名 | 倉庫名 |
| 移動先置場id | 倉庫間移動の場合の移動先ID |
| 移動先置場名 | 倉庫間移動の場合の移動先名 |
| 数量(pl) | 取引数量（PL） |
| 単価 | 単価（円/PL） |
| コスト | コスト（円） |
| created_at | 実行日時 |

## 倉庫区分

- **区分1（保管専用）**: 出庫禁止の倉庫。出荷場所（区分2）への倉庫間移動が必要。
- **区分2（保管&出荷可能）**: 直接外部出庫が可能な倉庫。

## 倉庫別PSI計算ロジック

### データ開始月
- BeginInv_倉庫 = BeginInv_pl × (EndInv_倉庫 / EndInv_pl)
- In_倉庫 = In_pl × (EndInv_倉庫 / EndInv_pl)
- Sales_倉庫 = BeginInv_倉庫 + In_倉庫 - EndInv_倉庫（逆算）

### 新規倉庫（前月在庫なし）
- BeginInv_倉庫 = 0
- In_倉庫 = EndInv_倉庫（全量入庫）
- Sales_倉庫 = 0

### 既存倉庫
- BeginInv_倉庫 = 前月のEndInv_倉庫
- Sales_倉庫 = Sales_pl × (BeginInv_倉庫 / 実際のBeginInv_pl)
- In_倉庫 = EndInv_倉庫 + Sales_倉庫 - BeginInv_倉庫（逆算）

## 倉庫間移動の扱い

区分1の倉庫からの倉庫間移動は**出荷のための移動**であり、PSI計算には含まれません。

**fbl（区分2、出荷場所）の例:**
```
BeginInv + 入庫（外部） + 倉庫間入庫 - 出庫（外部） - 倉庫間出庫 = EndInv
```

倉庫間入庫と倉庫間出庫は相殺されるため、純粋なPSIは：
```
BeginInv + 入庫（外部） - 出庫（外部） = EndInv
```

## ディレクトリ構成

```
.
├── main.py                          # メインエントリーポイント
├── config/
│   └── kintone_api_setting.yaml    # Kintone API設定
├── util/
│   ├── kintone_fetch.py             # Kintone データ取得
│   ├── kintone_data_loader.py       # マスタデータロード
│   ├── monthly_inventory.py         # 需要→在庫換算
│   ├── psimake.py                   # PSI計算
│   ├── optimizer.py                 # 在庫配置最適化
│   ├── report.py                    # 取引明細レポート生成
│   └── save.py                      # CSV出力
├── DATABRICKS_SETUP.md              # Databricksセットアップガイド
├── databricks_notebook_example.py   # Databricksノートブックサンプル
└── requirements.txt                 # Python依存関係
```

## 依存関係

- pandas
- pyyaml
- requests
- pulp（線形計画法ソルバー）
- python-dotenv（ローカル開発用）

## ライセンス

(社内利用のため省略)

## 開発者向け

### ローカル開発環境のセットアップ

```bash
# 仮想環境作成
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 依存関係インストール
pip install -r requirements.txt

# .env作成
cp .env.example .env
# .envを編集してKINTONE_BASE_URLを設定
```

### コミット規約

```
feat: 新機能
fix: バグ修正
refactor: リファクタリング
docs: ドキュメント更新
test: テスト追加・修正
```
