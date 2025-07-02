# Image RAG System with Cohere Embed v4

Cohere Embed Multimodal v4による自然言語画像検索システム

## 概要

このシステムは、Cohere Embed v4 APIを使用して画像の埋め込みベクトルを生成し、自然言語クエリで最適な画像を検索できるRAG（Retrieval-Augmented Generation）システムです。

## 使用方法

### 0. 環境構築（uvのインストール必須）
```bash
uv sync
. .venv/bin/activate
```

### 1. 画像処理（初回/新しい画像追加時）
```bash
# 画像をRAGデータベースに追加（重複スキップ機能付き）
python image_processor.py
```

### 2. 画像検索
```bash
# 対話型検索
python interactive_search.py
```

### 3. プログラムから使用
```python
from search import ImageSearcher

searcher = ImageSearcher()
results = searcher.search_images("モダンなリビングルーム", top_k=3)
searcher.print_search_results(results)
```

## ファイル構成

- `image_processor.py` - 画像処理専用（RAGデータベース構築）
- `search.py` - 画像検索ライブラリ
- `interactive_search.py` - 対話型検索インターフェース
- `embeddings.json` - 生成済み埋め込みデータ
- `images/` - 画像ディレクトリ

## コスト分析

- **画像埋め込み**: $0.0001/画像
- **テキスト検索**: $0.12/1M tokens

## 制限事項

- 画像サイズ上限: 20MB
- 対応形式: PNG, JPEG, WebP, GIF
- バッチ処理: 1画像/APIコール

## 今後の拡張

1. Google Drive API統合  
2. 画像リサイズ機能
3. ベクトルDB（Pinecone/Weaviate）統合
4. Web UI実装