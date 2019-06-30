import os
import sys
import csv

def main():
    with open('Project/data/Zaim.20190630115341.csv', 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)  # ヘッダーを読み飛ばしたい時

        for row in reader:
            print (row)          # 1行づつ取得できる


if __name__ == '__main__':
    main()
