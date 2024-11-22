import os
import csv

def save_csv(file_path, headers, data):
    """
    データをCSV形式で保存する関数。

    Parameters:
        file_path (str): 保存するCSVファイルのパス。
        headers (list): CSVのヘッダー。
        data (list of list): CSVに保存するデータ。

    Returns:
        str: 保存したCSVファイルのパス。
    """
    try:
        # ディレクトリを作成
        directory = os.path.dirname(file_path)
        if not os.path.exists(directory):
            os.makedirs(directory)

        with open(file_path, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(headers)  # ヘッダーを書き込み
            writer.writerows(data)  # データを書き込み
        return file_path
    except Exception as e:
        raise IOError(f"Failed to save CSV: {e}")


def save_sql(file_path, sql_content):
    """
    SQLクエリを .sql ファイルとして保存する関数。

    Parameters:
        file_path (str): 保存する .sql ファイルのパス。
        sql_content (str): 保存する SQL クエリの文字列。

    Returns:
        str: 保存した .sql ファイルのパス。
    """
    try:
        # ディレクトリを作成（存在しない場合のみ）
        directory = os.path.dirname(file_path)
        if not os.path.exists(directory):
            os.makedirs(directory)

        # .sql ファイルに書き込む
        with open(file_path, mode='w', encoding='utf-8') as file:
            file.write(sql_content)

        return file_path
    except Exception as e:
        raise IOError(f"Failed to save SQL file: {e}")


def save_log(file_path, log_content):
    """
    logファイルを保存する関数。

    Parameters:
        file_path (str): 保存する .log ファイルのパス。
        log_content (str): 保存する文字列。

    Returns:
        str: 保存した .sql ファイルのパス。
    """
    try:
        # ディレクトリを作成（存在しない場合のみ）
        directory = os.path.dirname(file_path)
        if not os.path.exists(directory):
            os.makedirs(directory)

        # .log ファイルに書き込む
        with open(file_path, mode='w', encoding='utf-8') as file:
            file.write(log_content)

        return file_path
    except Exception as e:
        raise IOError(f"Failed to save LOG file: {e}")
