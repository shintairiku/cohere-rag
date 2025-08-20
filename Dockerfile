# ベースイメージとして公式のPython 3.11スリム版を使用
FROM python:3.11-slim

# 環境変数設定
ENV PYTHONUNBUFFERED=True

# アプリケーションを配置する作業ディレクトリを作成
WORKDIR /app

# 最初にrequirements.txtをコピーしてライブラリをインストール
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# アプリケーションのソースコードとデータファイルをコピー
COPY . .

# コンテナがリッスンするポートを指定
EXPOSE 8080

# コンテナ起動時に実行するコマンド (Uvicornを使用)
# UvicornをWebサーバーとして使用し、main.py内の'app'インスタンスを起動します
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]

