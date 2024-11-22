import mysql.connector
import time
import json  # 必要な場合のため追加
# from config import get_db_config
from get_sql import get_sql_query_from_csv
from save_files import save_csv, save_sql, save_log
from create_contents import convert_json_to_csv, create_log_content

# 共通の MySQL 接続設定テンプレート
DB_CONFIG_TEMPLATE = {
    "user": "smaad",
    "password": "tech1204"
}

# ホストを条件分岐して接続設定を作成
def get_db_config(host):
    if host == "writer":
        return {
            **DB_CONFIG_TEMPLATE,
            "host": "offerwall-test-mysql80-prod-cluster.cluster-ckab3gizbyr1.ap-northeast-1.rds.amazonaws.com"
        }
    elif host == "reader":
        return {
            **DB_CONFIG_TEMPLATE,
            "host": "offerwall-test-mysql80-prod-cluster.cluster-ro-ckab3gizbyr1.ap-northeast-1.rds.amazonaws.com"
        }
    else:
        raise ValueError("Invalid host parameter")

class MySQLConnectionManager:
    def __init__(self, host_type):
        self.db_config = get_db_config(host_type)
        self.connection = None

    def __enter__(self):
        # MySQLに接続
        self.connection = mysql.connector.connect(**self.db_config)
        return self.connection

    def __exit__(self, exc_type, exc_value, traceback):
        # 接続をクローズ
        if self.connection and self.connection.is_connected():
            self.connection.close()

def execute_sql(query, connection):
    try:
        cursor = connection.cursor()

        # クエリ実行時間を測定
        start_time = time.time()
        cursor.execute(query)
        results = cursor.fetchall()
        end_time = time.time()

        # 列名を取得
        headers = [desc[0] for desc in cursor.description]

        # 実行結果
        execution_time = (end_time - start_time) * 1000  # ms
        response = {
            "rowCount": len(results),
            "executionTime": execution_time,
            "startTime": start_time,
            "headers": headers,
            "data": results
        }

        return response

    except mysql.connector.Error as err:
        return {"error": str(err)}

    finally:
        # Cursorをクローズ
        if cursor:
            cursor.close()

def execute_sql_queries(sql_type):
    input_csv_file_path = "./input/offerwall_batch_sql_check.csv" if sql_type == "batch" else "./input/offerwall_sql_check.csv"
    output_folder_path = f"./output/{sql_type}"

    with open("src/config.json", "r") as file:
        config = json.load(file)
        row_value = config.get("row_value", "")

    if not row_value:
        print('ERROR: "row" is empty.')
        return
    
    unique_values = (
        row_value.split("_") if "_" in row_value else [row_value]
    )
    unique_values = sorted([int(v) for v in unique_values])

    # 接続を管理するクラスを使用
    with MySQLConnectionManager(host_type="writer") as connection:
        for row in unique_values:
            print(f"========== START PROCESSING: Row {row} ==========")
            query = get_sql_query_from_csv(input_csv_file_path, row)

            for count in range(3):
                print(f"[ExecutionCount:{count + 1}]")
                result = execute_sql(query, connection)

                if "error" in result:
                    print(f"Error: {result['error']}")
                    break

                # CSVファイルの保存
                if count == 0 and result["data"]:
                    csv_content = convert_json_to_csv(result["headers"], result["data"])
                    csv_file_path = f"{output_folder_path}/Row{row}/SQL80.csv"
                    save_csv(csv_file_path, result["headers"], result["data"])
                    print(f"Saved CSV to {csv_file_path}")

                    # SQLファイルの保存
                    sql_file_path = f"{output_folder_path}/Row{row}/SQL80.sql"
                    save_sql(sql_file_path, query)
                    print(f"Saved SQL to {sql_file_path}")

                # ログファイルの保存
                log_content = create_log_content(result["startTime"], query, result["rowCount"], result["executionTime"])
                log_file_path = f"{output_folder_path}/Row{row}/SQL80-{count + 1}.log"
                save_log(log_file_path, log_content)
                print(f"Saved Log to {log_file_path}")

            # 平均実行時間の計算
            average_execution_time = round(sum([result["executionTime"] for _ in range(3)]) / 3 / 1000, 3)
            print(f"Average Execution Time for Row {row}: {average_execution_time} ms")

            print(f"========== END PROCESSING: Row {row} ==========")
