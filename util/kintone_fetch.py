import requests
import os
import pandas as pd
import yaml
from typing import Dict, Iterable, Optional, Tuple, List

os.environ["http_proxy"] = "http://agcproxy:7080"
os.environ["https_proxy"] = "http://agcproxy:7080"


def load_yaml_config(file_name):
    """YAML ファイルを読み込む関数"""
    try:
        # 現在のスクリプトのディレクトリから config ディレクトリへのパスを生成
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        # configディレクトリ内のファイルを指定
        full_path = os.path.join(base_dir, 'config', file_name)
        
        with open(full_path, 'r', encoding='utf-8') as file:
            return yaml.safe_load(file)
    except Exception as e:
        print(f"設定ファイルの読み込みエラー: {e}")
        raise

# 設定ファイルの読み込み
config_path = 'kintone_api_setting.yaml'  # ファイル名のみを指定
API_SETTINGS = load_yaml_config(config_path)


class KintoneDataManager:
    def __init__(self):
        # 設定読み込み

        settings = API_SETTINGS
        self.base_url: str = settings["base_url"]
        self.configs: Dict[str, Dict[str, str]] = settings["configs"]

        # ★キャッシュ用（必須）
        self.dataframes: Dict[str, pd.DataFrame] = {}
        self._fetched_keys: set[str] = set()

        # リクエスト共通
        self._timeout = (10, 60)  # (connect, read)
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "KintoneDataManager/1.0"})

    def get_dataframe(self, name: str) -> pd.DataFrame:
        """キャッシュされた1つの DataFrame を返す"""
        if not hasattr(self, "dataframes") or not self.dataframes:
            raise ValueError("まだ何もフェッチしていません。fetch_selected(...) を先に呼んでください。")
        return self.dataframes.get(name.strip())

    def get_multiple_dataframes(self, *names: str):
        """複数の DataFrame をまとめて返す"""
        if not hasattr(self, "dataframes") or not self.dataframes:
            raise ValueError("まだ何もフェッチしていません。fetch_selected(...) を先に呼んでください。")
        return [self.get_dataframe(n) for n in names]

    def get_available_dataframes(self):
        """現在キャッシュされている DataFrame 名一覧"""
        if not hasattr(self, "dataframes") or not self.dataframes:
            return []
        return list(self.dataframes.keys())


    # ====== Public API ======
    def fetch_data(self):
        """後方互換：configにある全テーブルを取得"""
        return self.fetch_selected(self.configs.keys())

    def fetch_selected(
        self,
        names: Iterable[str],
        override_queries: Optional[Dict[str, str]] = None,
        force: bool = False,
        fields_per_name: Optional[Dict[str, List[str]]] = None,   # ★ 追加
        limit: int = 500,
    ):
        """
        指定した config 名だけ取得
        - names: ["dim", "proc", "date"] のように指定
        - override_queries: {"date": "..."} でクエリ上書き
        - force=True で再取得
        - fields_per_name: {"dim": ["品目","品種"], "proc": ["品種","品種分類"]} のように返却列を限定  ★
        """
        names = [n.strip() for n in names]
        override_queries = override_queries or {}
        fields_per_name = fields_per_name or {}

        # まだ取っていない or force のものだけ対象にする
        target_names = [n for n in names if (force or n not in self._fetched_keys)]
        if not target_names:
            return self  # 既に全部キャッシュ済み

        for key in target_names:
            if key not in self.configs:
                raise KeyError(f"configsに '{key}' が見つかりません。")

            cfg = self.configs[key]
            app_no   = cfg["app_no"]
            apitoken = cfg["apitoken"]
            query    = override_queries.get(key, cfg.get("query", ""))

            # ★ このアプリ用に指定されたフィールド（なければ None = 全列）
            fields = fields_per_name.get(key)
            # `$id` は後続の subtable 結合等で使うので自動で付与
            if fields is not None and "$id" not in fields:
                fields = ["$id"] + list(fields)

            main_df, sub_dfs = self._fetch_kintone_records(app_no, apitoken, query=query, limit=limit, fields=fields)

            # キャッシュ格納（main）
            self.dataframes[f"{key}_main"] = main_df

            # キャッシュ格納（sub）
            for i, (sub_name, sub_df) in enumerate(sub_dfs.items(), start=1):
                merged_df = sub_df.merge(main_df, how="left", left_on="main_id", right_on="$id")
                self.dataframes[f"{key}_{sub_name}"] = merged_df

            self._fetched_keys.add(key)

        self._print_summary(names)
        return self

    # ====== Internal ======
    def _fetch_kintone_records(
        self,
        app_no: int,
        apitoken: str,
        query: str = '',
        limit: int = 500,
        fields: Optional[List[str]] = None,   # ★ 追加
    ) -> Tuple[pd.DataFrame, Dict[str, pd.DataFrame]]:
        headers = {"X-Cybozu-API-Token": apitoken}
        offset = 0
        records = []
        total = 0

        while True:
            params = {"app": app_no, "query": f"{query} limit {limit} offset {offset}"}

            # ★ 返却フィールドを限定（カラム数を減らして軽量化）
            if fields:
                for f in fields:
                    # Kintone の仕様に合わせて fields[] を複数付与
                    params.setdefault("fields[]", []).append(f)

            ret = self._session.get(self.base_url, headers=headers, params=params, timeout=self._timeout)
            if ret.status_code != 200:
                # デバッグ用: print(ret.text)
                ret.raise_for_status()
            batch = ret.json().get("records", [])
            if not batch:
                break
            records.extend(batch)
            got = len(batch)
            total += got
            offset += limit

            if offset >= 10000:
                break

        main_df, sub_dfs = self._process_records(records)
        return main_df, sub_dfs

    def _process_records(self, records):
        main_data = []
        sub_data_dict = {}  # key=フィールド名, value=list[dict]

        for record in records:
            main_row = {}
            for key, value in record.items():
                if value["type"] == "SUBTABLE":
                    self._process_subtable(record, key, value, sub_data_dict)
                else:
                    self._process_main_field(main_row, key, value)
            main_data.append(main_row)

        df_main = pd.DataFrame(main_data)
        df_sub_tables = {
            f"df_sub_{i+1}": pd.DataFrame(data) for i, (k, data) in enumerate(sub_data_dict.items())
        }
        return df_main, df_sub_tables

    def _process_main_field(self, main_row, key, value):
        if value["type"] in ["CREATOR", "MODIFIER"]:
            main_row[f"{key}_code"] = value["value"]["code"]
            main_row[f"{key}_name"] = value["value"]["name"]
        elif isinstance(value.get("value"), dict):
            for subkey, subvalue in value["value"].items():
                main_row[f"{key}_{subkey}"] = subvalue
        else:
            main_row[key] = value.get("value")

    def _process_subtable(self, record, key, value, sub_data_dict):
        if key not in sub_data_dict:
            sub_data_dict[key] = []
        for subrec in value["value"]:
            sub_row = {subkey: subvalue["value"] for subkey, subvalue in subrec["value"].items()}
            sub_row["main_id"] = record["$id"]["value"]
            sub_data_dict[key].append(sub_row)

    def _print_summary(self, names: Iterable[str]):
        print("取得完了")