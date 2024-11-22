import sys
from execute_sql import execute_sql_queries


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Error: No query type provided.")
        print("Usage: python main.py <query_type>")
        sys.exit(1)

    # 引数からSQLタイプを取得
    query_type = sys.argv[1]

    # SQLタイプに応じて処理を分岐
    if query_type == "batch" or query_type == "backend":
        execute_sql_queries(query_type)
    # elif query_type == "backend":
    #     execute_sql_query(query_type)
    else:
        print(f"Error: Invalid query type '{query_type}'. Valid options are 'batch' or 'backend'.")
