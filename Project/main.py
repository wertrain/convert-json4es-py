import os
import sys
import csv
import json
import requests
import pandas

CSV_FILE = 'Project/data/Zaim.20190630115341.csv'

def make_json_4_bulk_api():
    # CSV から読み込み
    df = pandas.read_csv(CSV_FILE)
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
    es_index = '{"index" : {"_index":"zaim", "_type":"payment"}}\n'
    # リストをすべて処理
    for row in df_list:
        # Elasticsearch に登録できる JSON に
        es_str = es_index + row + '\n'
        # 登録
        post_es(es_str.encode('utf-8'))

def main():
    data_list = []
    with open(CSV_FILE, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader) # ヘッダーの読み飛ばし
        for row in reader:
            data = {}
            data['time'] = row[0]
            data['category'] = row[2]
            data['subcatergory'] = row[3]
            data['payment'] = row[4]
            data['shop'] = row[8]
            data['price'] = row[11]
            data_list.append(data)
            #print (data) 

    #with open('outputnp.json', 'w') as f:
    #    f.write(json.dumps(data_list, ensure_ascii=False))

ES_LOCAL_IP = 'http://192.168.220.130:9200'
ES_GET_DOC = '/library/_doc/1'
ES_POST_BULK = '/_bulk'

def get_es():
    r = requests.get(ES_LOCAL_IP + ES_GET_DOC)
    print (json.loads(r.text))

def post_es(data):
    headers = {'Content-Type': 'application/json'} # 
    r = requests.post(ES_LOCAL_IP + ES_POST_BULK + '?pretty', data, headers=headers)
    print (json.loads(r.text))
    
if __name__ == '__main__':
    #main()
    make_json_4_bulk_api()
