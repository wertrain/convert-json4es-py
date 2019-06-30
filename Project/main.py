import csv
import json
import os
import sys

import pandas
import requests

CSV_FILE = 'Project/data/Zaim.20190630115341.csv'
ES_INDEX_NAME = 'zaim'
ES_TYPE_NAME = 'payment'

ES_LOCAL_IP = 'http://192.168.220.130:9200'
ES_GET_INDEX_ZAIM = '/' + ES_INDEX_NAME
ES_GET_INDEX_ZAIM_TYPE_PAYMENT = '/' + ES_INDEX_NAME + '/' + ES_TYPE_NAME
ES_POST_BULK = '/_bulk'
ES_SEARCH = ES_GET_INDEX_ZAIM_TYPE_PAYMENT + '/_search?pretty'

#
# Elasticsearch の API をリクエストする
#
def request_elasticsearch():
    r = requests.get(ES_LOCAL_IP + ES_SEARCH)
    return json.loads(r.text)

#
# Elasticsearch にデータを登録する
#
def post_to_elasticsearch(data):
    headers = {'Content-Type': 'application/json'} # 
    r = requests.post(ES_LOCAL_IP + ES_POST_BULK + '?pretty', data, headers=headers)
    return json.loads(r.text)

#
# 読み込んだ csv を Elasticsearch に登録する
#
def post_zaim_csv_to_elasticsearch(csv_file_path):
    # CSV から読み込み
    df = pandas.read_csv(csv_file_path)
    # 不要な列を削除
    df = df.drop('通貨', axis=1)
    df = df.drop('収入', axis=1)
    df = df.drop('振替', axis=1)
    df = df.drop('残高調整', axis=1)
    df = df.drop('通貨変換前の金額', axis=1)
    df = df.drop('集計の設定', axis=1)
    df = df.drop('メモ', axis=1)
    df = df.drop('入金先', axis=1)
    df = df.drop('品目', axis=1)
    df = df.drop('方法', axis=1)
    # 項目のリネーム
    df = df.rename(columns={
        '日付':'date',
        'カテゴリ':'category',
        'カテゴリの内訳':'subcatergory',
        '支払元':'payment',
        'お店':'shop',
        '支出':'price',
    })
    # JSON テキストへ変換
    df_str = df.to_json(force_ascii=False, orient='records', lines=True)
    #print (df_str)
    # リスト形式へ
    df_list = df_str.split('\n')
    # インデックスを作成
    es_index = '{"index" : {"_index":"' + ES_INDEX_NAME + '", "_type":"' + ES_TYPE_NAME + '"}}\n'
    # リストをすべて処理
    for row in df_list:
        # Elasticsearch に登録できる JSON に
        es_str = es_index + row + '\n'
        # 登録
        #post_to_elasticsearch(es_str.encode('utf-8'))
        print (es_str)

def main():
    post_zaim_csv_to_elasticsearch(CSV_FILE)
    #print (request_elasticsearch())

if __name__ == '__main__':
    main()
