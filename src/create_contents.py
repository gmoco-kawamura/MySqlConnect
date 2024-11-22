import json
import re
import datetime

def convert_json_to_csv(headers, result):
    """
    タプル形式のデータをCSV形式に変換する関数。

    Parameters:
        headers (list): CSVのヘッダー(列名）。
        result (list): データ（タプルのリスト）。

    Returns:
        tuple: (headers, data) CSV形式のヘッダーとデータ。
    """
    if not headers or not result:
        raise ValueError("Headers and result cannot be empty.")

    # ISO 8601 日付の正規表現パターン
    iso_date_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")

    # データ行を作成
    data = []
    for row in result:
        values = []
        for value in row:
            if value is None:
                values.append("NULL")  # NULLの処理
            elif isinstance(value, dict):
                values.append(json.dumps(value).replace('"', '""'))  # JSON文字列として変換
            elif isinstance(value, str) and iso_date_pattern.match(value):
                # ISO 8601形式の場合はフォーマットを変更
                formatted_date = value.replace("T", " ").split(".")[0]
                values.append(formatted_date)
            else:
                # 改行やダブルクォーテーションのエスケープ処理
                cell_string = str(value).replace("\n", "\\n").replace('"', '""')
                values.append(cell_string)

        data.append(values)

    return headers, data


def create_log_content(start_time, sql_query, row_count, execution_time):
    """
    ログファイル用の文字列を作成する関数。

    Parameters:
        start_time (float): Unixタイムスタンプ。
        sql_query (str): 実行したSQLクエリ。
        row_count (int): クエリの結果として返された行数。
        execution_time (float): 実行時間（ミリ秒）。

    Returns:
        str: ログファイルに記録するフォーマット済み文字列。
    """
    # Unixタイムスタンプを hh:mm:ss の形式に変換
    formatted_time = datetime.datetime.fromtimestamp(start_time).strftime('%H:%M:%S')

    # SQLクエリ内の改行や余分な空白を削除
    cleaned_sql = ' '.join(sql_query.split())

    # 実行時間を秒単位に変換（ms → sec）
    execution_time_sec = round(execution_time / 1000, 3)

    # ログフォーマットに整形
    log_content = f"{formatted_time}\t{cleaned_sql}\t{row_count} row(s) returned\t{execution_time_sec} sec"

    return log_content
