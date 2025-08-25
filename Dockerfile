# syntax=docker/dockerfile:1.7

# Python 3.12（pyprojectの要件を満たす）
FROM python:3.12-slim

# 低レイヤ設定
ENV PYTHONUNBUFFERED=1 \
    # uvが作るプロジェクト仮想環境の場所
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    # uv & venv を PATH に通す
    PATH="/app/.venv/bin:/root/.local/bin:$PATH" \
    # コンテナでのリンク問題を避ける
    UV_LINK_MODE=copy

# 必要最低限のツール（uvインストール用）
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# uv をインストール
RUN curl -LsSf https://astral.sh/uv/install.sh | sh

WORKDIR /app

# 依存解決はキャッシュを最大化するためメタデータだけ先にコピー
COPY pyproject.toml ./
# ロックファイルがあれば一緒に（存在しない場合はスキップされる）
COPY uv.lock* ./

# 依存を同期（ロックがあれば --frozen、なければ通常sync）
# BuildKitがある場合は uv キャッシュを活用（任意）
# RUN --mount=type=cache,target=/root/.cache/uv \
#     bash -c 'test -f uv.lock && /root/.local/bin/uv sync --frozen --no-dev || /root/.local/bin/uv sync --no-dev'
RUN bash -c 'test -f uv.lock && /root/.local/bin/uv sync --frozen --no-dev || /root/.local/bin/uv sync --no-dev'

# アプリ本体
COPY . .

# Cloud Run 既定
EXPOSE 8080

# uv経由でuvicornを起動（.venvが確実に使われる）
CMD ["/root/.local/bin/uv", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]