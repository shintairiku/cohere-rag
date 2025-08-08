# Dockerfile for Indexing Service

# 1. ベースイメージの指定
FROM python:3.12-slim

# 2. Cloud SDKのインストール (gcloud CLI)
#    AlloyDB Connectorが内部で利用するため
RUN apt-get update && \
    apt-get install -y curl gnupg && \
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] [https://packages.cloud.google.com/apt](https://packages.cloud.google.com/apt) cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl [https://packages.cloud.google.com/apt/doc/apt-key.gpg](https://packages.cloud.google.com/apt/doc/apt-key.gpg) | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - && \
    apt-get update -y && apt-get install google-cloud-sdk -y

# 3. 環境変数の設定
ENV PYTHONUNBUFFERED True
ENV APP_HOME /app
WORKDIR $APP_HOME

# 4. 依存関係のインストール
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. ソースコードのコピー
COPY indexing_main.py .

# 6. 実行コマンド
CMD ["python", "indexing_main.py"]