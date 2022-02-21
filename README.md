# 本プログラム(User_IDごとのセッションをグループ化)の実行について

## Set Upについて
```bash
詳細は下記のサイトから参照して各モジュールをインストールを行うこと
https://medium.com/tinghaochen/how-to-install-pyspark-locally-94501eefe421
# Install Python or Python3(本プログラムをPython3で) 

# Download Spark

# Install pyspark

# Change the execution path for pyspark
```

## プログラムの実行について
```bash
# CSV dataをローカルに用意して、calculate_session.pyに渡すこと。デフォルトは、「/Users/xiaoshuang.xu/Documents/J_プログラミングテスト/example_data_2019_v1.csv」になる

# usage: calculate_session.py [-f CSV input file]

# cd {calculate_session.pyの置きディレクトリ}

# python3 calculate_session.py -f {CSV input file}

# 実行結果を確認する(session_durationを秒数で表示)
```


## UnitTestのについて(今回時間のため、省略させていただく)
```bash
pwd 
# cd {testの置きディレクトリ}
make test

#single Unittestの実行
python3 -m pytest -vv {test target}
```

## Update Libraries
```bash
pip3 freeze > requirements.txt
```
