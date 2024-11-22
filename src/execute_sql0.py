import mysql.connector
import time
import json
import save_files
import create_contents
# from execute_sql import execute_sql
from get_sql import get_sql_query_from_csv


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


# SQL クエリを実行する関数
def execute_sql(query, host):
    connection = None
    try:
        # 接続設定を取得
        db_config = get_db_config(host)

        # MySQL に接続
        connection = mysql.connector.connect(**db_config)
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
        # connection と cursor の存在を確認してからクローズ
        if connection and connection.is_connected():
            if cursor:
                cursor.close()
            connection.close()


def execute_sql_queries(sql_type):
    """
    SQLクエリを実行するPython関数。
    
    Parameters:
        

    Returns:
        None
    """

    if (sql_type == "batch"):
        input_csv_file_path = "./input/offerwall_batch_sql_check.csv"
    else:
        input_csv_file_path = "./input/offerwall_sql_check.csv"

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

    for row in unique_values:
        print(f"========== START PROCESSING: Row {row} ==========")
        input_csv_file_path = "./input/offerwall_batch_sql_check.csv"   # TODO
        output_csv_file_path = f"./output/{sql_type}/Row{row}/SQL80.csv"
        output_query_file_path = f"./output/{sql_type}/Row{row}/SQL80.sql"

        # クエリの取得
        query = get_sql_query_from_csv(input_csv_file_path, row) # TODO row_numberの定義

        # host_typeの決定
        if query.strip().lower().startswith("select"):
            host_type = "reader"
        else:
            host_type = "writer"

        for count in range(3):
            print(f"[ExecutionCount:{count + 1}]")
            output_log_file_path = f"./output/{sql_type}/Row{row}/SQL80-{count + 1}.log"
            total_execution_time = 0

            # クエリを実行
            result = execute_sql(query, host_type)
            if "error" in result:
                print(f"Error: {result['error']}")
                break

            # print(f"Result Data: {result['data']}")
            print(f"Row Count: {result['rowCount']}")
            print(f"Execution Time: {result['executionTime']}ms")
            print(f"Start Time: {result['startTime']}")

            total_execution_time += result["executionTime"]

            if count == 0:
                # 結果をcsv保存
                headers, data = create_contents.convert_json_to_csv(result['headers'], result['data'])
                saved_csv_path = save_files.save_csv(output_csv_file_path, headers, data)
                print(f"CSV saved to: {saved_csv_path}")

                # クエリを保存
                saved_sql_path = save_files.save_sql(output_query_file_path, query)
                print(f"SQL saved to: {saved_sql_path}")
        
            # ログを保存
            log_content = create_contents.create_log_content(
                start_time=result['startTime'],
                sql_query=query,
                row_count=result['rowCount'],
                execution_time=result['executionTime']
            )
            saved_log_path = save_files.save_log(output_log_file_path, log_content)
            print(f"LOG saved to: {saved_log_path}")

        # 平均実行時間
        average_execution_time_sec = round(total_execution_time / 3 / 1000, 3)
        print(f"平均実行時間：{average_execution_time_sec}sec")
        print(f"=========== END PROCESSING: Row {row} ===========")