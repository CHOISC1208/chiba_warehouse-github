# Databricks セットアップガイド

## 1. Databricks Secretsの設定

### Secretsの作成
```bash
# Databricks CLIを使用する場合
databricks secrets create-scope --scope kintone
databricks secrets put --scope kintone --key KINTONE_BASE_URL
```

または、Databricks UIから：
1. Settings → User Settings → Developer → Access Tokens
2. 左メニューから「Secrets」→「Create Secret Scope」
3. Scope名: `kintone`
4. Secret作成:
   - Key: `KINTONE_BASE_URL`
   - Value: `https://xxxx.cybozu.com/k/v1/records.json`

## 2. リポジトリのインポート

1. Databricks Workspace → Repos
2. 「Add Repo」をクリック
3. Git URLを入力: `https://github.com/CHOISC1208/chiba_warehouse-github.git`
4. ブランチ: `claude/env-api-url-config-WPg2k`（または`main`）

## 3. 需要データのアップロード

### 方法A: DBFSに直接アップロード
```python
# Databricks UIから
# 左メニュー → Data → Add → File Upload
# アップロード先: /FileStore/warehouse_optimizer/input/20251208.csv
```

### 方法B: ノートブックからアップロード
```python
# ノートブックで実行
uploaded_files = dbutils.fs.put("/FileStore/warehouse_optimizer/input/20251208.csv",
                                  open("20251208.csv").read(), True)
```

## 4. ノートブックの実行

### 新規ノートブック作成
1. Workspace → Create → Notebook
2. 以下のコードをコピー＆ペースト:

```python
# セル1: ライブラリインストール
%pip install pulp python-dotenv

# セル2: インポート
import sys
sys.path.append("/Workspace/Repos/<your-username>/chiba_warehouse-github")
from main import run_optimization

# セル3: パス指定
dbutils.widgets.text("csv_path", "/dbfs/FileStore/warehouse_optimizer/input/20251208.csv", "需要CSVパス")
demand_csv_path = dbutils.widgets.get("csv_path")

# セル4: 実行
psi_df, allocation_summary_df = run_optimization(demand_csv_path)

# セル5: 結果確認
display(psi_df)
display(allocation_summary_df)

# セル6: 保存（オプション）
output_path = "/dbfs/FileStore/warehouse_optimizer/output/"
psi_df.to_csv(f"{output_path}psi.csv", index=False, encoding="utf-8-sig")
allocation_summary_df.to_csv(f"{output_path}allocation_summary.csv", index=False, encoding="utf-8-sig")
```

## 5. 結果のダウンロード

### DBFSから直接ダウンロード
```python
# ノートブックで実行
from IPython.display import FileLink
FileLink('/dbfs/FileStore/warehouse_optimizer/output/psi.csv')
FileLink('/dbfs/FileStore/warehouse_optimizer/output/allocation_summary.csv')
```

または、Databricks UIから：
1. 左メニュー → Data → DBFS
2. `/FileStore/warehouse_optimizer/output/` に移動
3. ファイルを右クリック → Download

## 6. トラブルシューティング

### エラー: ModuleNotFoundError: No module named 'databricks'
**原因**: ローカル環境で実行しようとしている
**解決**: Databricks環境で実行するか、`.env`ファイルを作成

### エラー: FileNotFoundError: data/20251208.csv
**原因**: 需要CSVが見つからない
**解決**: `demand_csv_path` パラメータを正しく指定

### エラー: Invalid URL '${KINTONE_BASE_URL}'
**原因**: Databricks Secretsが設定されていない
**解決**: Secretsの設定を確認（手順1を参照）

## 7. 定期実行の設定（オプション）

### Jobの作成
1. Databricks Workspace → Workflows → Jobs
2. 「Create Job」をクリック
3. Task設定:
   - Type: Notebook
   - Path: 作成したノートブックのパス
   - Cluster: 使用するクラスター
4. Schedule設定:
   - Trigger: Scheduled
   - Cron: `0 0 1 * *`（毎月1日0時に実行）

## 8. コスト最適化

月1回の実行の場合：
- **クラスタータイプ**: Single Node
- **インスタンスタイプ**: Standard_DS3_v2（または最小スペック）
- **Auto Terminate**: 30分
- **推定コスト**: 月数百円程度

## 付録: ローカル開発環境

Databricksにデプロイする前にローカルでテストする場合：

```bash
# 1. .envファイル作成
echo "KINTONE_BASE_URL=https://xxxx.cybozu.com/k/v1/records.json" > .env

# 2. 依存関係インストール
pip install -r requirements.txt

# 3. 実行
python main.py
```
