import csv

def get_sql_query_from_csv(file_path, row_number):
    """
    CSVファイルから指定された1行のSQLクエリを取得する関数。

    Parameters:
        file_path (str): CSVファイルのパス。
        row_number (int): 取得したい行番号。

    Returns:
        str: 指定された行のSQLクエリ。
    """
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.reader(file)
        for i, row in enumerate(reader, start=1):  # CSVは1行目が1
            if i == row_number:
                try:
                    return row[8]  # I列（0始まりの8番目）
                except IndexError:
                    return ""  # I列が空の場合は空文字を返す
    return None  # 指定された行がCSVに存在しない場合
