"""
画像処理専用スクリプト - RAGデータベース構築
（共有ドライブの自動監視機能付き）
"""

import os
import base64
import json
import hashlib
from pathlib import Path
from typing import List, Dict, Set
from dotenv import load_dotenv
import cohere
import numpy as np
import sys
import time
import argparse
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

load_dotenv()

class ImageProcessor:
    def __init__(self, images_dir: str = "images", embeddings_file: str = "embeddings.json"):
        self.images_dir = Path(images_dir)
        self.embeddings_file = embeddings_file
        self.api_key = os.getenv("COHERE_API_KEY")
        self.image_extensions = {'.jpg', '.jpeg', '.png', '.webp', '.gif'}

        if not self.api_key:
            raise ValueError("COHERE_API_KEY not found in environment variables")

        self.client = cohere.ClientV2(api_key=self.api_key)
        self.processed_images: Set[str] = set()
        self.embeddings_data: List[Dict] = []

        self.load_existing_data()

    def load_existing_data(self):
        """既存の埋め込みデータを読み込み"""
        if os.path.exists(self.embeddings_file):
            try:
                with open(self.embeddings_file, 'r', encoding='utf-8') as f:
                    self.embeddings_data = json.load(f)
                for item in self.embeddings_data:
                    if 'file_hash' in item:
                        self.processed_images.add(item['file_hash'])
                print(f"📁 既存データ読み込み: {len(self.embeddings_data)}件")
            except Exception as e:
                print(f"❌ 既存データ読み込みエラー: {e}")
        else:
            print("📂 新規データベースを作成します")

    def show_status(self):
        """現在の状況を表示"""
        print(f"\n📊 画像処理システム状況:")
        print(f"  📂 対象ディレクトリ: {self.images_dir}")
        print(f"  💾 データベースファイル: {self.embeddings_file}")
        print(f"  📈 処理済み画像数: {len(self.embeddings_data)}件")

        if self.embeddings_data:
            print(f"\n📄 処理済み画像一覧 (一部):")
            for i, item in enumerate(self.embeddings_data[:5]):
                file_size_mb = item.get('file_size', 0) / (1024 * 1024)
                print(f"  - {item['filename']} ({file_size_mb:.2f}MB)")
            if len(self.embeddings_data) > 5:
                print(f"  ...他{len(self.embeddings_data) - 5}件")

    def get_file_hash(self, file_path: Path) -> str:
        """ファイルのハッシュ値を計算（重複検出用）"""
        hash_sha256 = hashlib.sha256()
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_sha256.update(chunk)
            return hash_sha256.hexdigest()
        except IOError as e:
            print(f"    ⚠️ ファイル読み込みエラー（ハッシュ計算中）: {file_path.name} - {e}")
            return None


    def image_to_base64_data_url(self, image_path: Path) -> str:
        """画像をbase64データURLに変換"""
        with open(image_path, "rb") as image_file:
            base64_bytes = base64.b64encode(image_file.read())
            base64_string = base64_bytes.decode('utf-8')
            ext = image_path.suffix.lower()
            mime_type = {
                '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.png': 'image/png',
                '.webp': 'image/webp', '.gif': 'image/gif'
            }.get(ext, 'image/jpeg')
            return f"data:{mime_type};base64,{base64_string}"

    def get_image_embedding(self, image_path: Path) -> np.ndarray:
        """画像の埋め込みベクトルを生成"""
        try:
            base64_url = self.image_to_base64_data_url(image_path)
            image_input = {"content": [{"type": "image_url", "image_url": {"url": base64_url}}]}
            response = self.client.embed(
                model="embed-v4.0", inputs=[image_input],
                input_type="search_document", embedding_types=["float"]
            )
            return np.array(response.embeddings.float_[0])
        except Exception as e:
            print(f"❌ 埋め込み生成エラー ({image_path.name}): {e}")
            return None

    def process_new_images(self):
        """新しい画像のみを一括で処理"""
        if not self.images_dir.exists():
            raise FileNotFoundError(f"画像ディレクトリ '{self.images_dir}' が見つかりません。パスが正しいか確認してください。")
        
        print("\n⏳ 対象ディレクトリをスキャンして、画像ファイルを探しています...")
        all_files = []
        scanned_dirs = 0
        try:
            for root, _, files in os.walk(self.images_dir):
                scanned_dirs += 1
                sys.stdout.write(f"\r  - スキャン中: {scanned_dirs}個のフォルダを調査済...  現在の場所: {root}")
                sys.stdout.flush()
                for filename in files:
                    if filename.lower().endswith(tuple(self.image_extensions)):
                        all_files.append(Path(root) / filename)
        except Exception as e:
            print(f"\n❌ ディレクトリのスキャン中にエラーが発生しました: {e}")
            return
        
        print(f"\n  ▶️  スキャン完了。{len(all_files)}件の画像ファイルが見つかりました。既存データと照合します...")

        new_images = []
        for image_path in all_files:
            file_hash = self.get_file_hash(image_path)
            if file_hash and file_hash not in self.processed_images:
                new_images.append((image_path, file_hash))
        
        print(f"  ▶️  照合完了。新規画像: {len(new_images)}件")

        if not new_images:
            print("\n✅ 処理する新しい画像はありませんでした。")
            return
        
        print(f"\n🔄 新しい画像の処理を開始します...")
        processed_count = 0
        for i, (image_path, file_hash) in enumerate(new_images, 1):
            print(f"  - 処理中 {i}/{len(new_images)}: {image_path.name}")
            # process_single_imageメソッドを再利用
            if self.process_single_image(image_path, precomputed_hash=file_hash):
                 processed_count += 1

        if processed_count > 0:
            print(f"\n🎉 一括処理完了! 新たに{processed_count}件の画像を追加しました")
            print(f"📊 総画像数: {len(self.embeddings_data)}件")


    def process_single_image(self, image_path: Path, precomputed_hash: str = None) -> bool:
        """【新規追加】指定された単一の画像を処理する"""
        if not image_path.exists() or image_path.suffix.lower() not in self.image_extensions:
            return False

        print(f"  - ファイルチェック: {image_path.name}")
        file_hash = precomputed_hash or self.get_file_hash(image_path)
        if not file_hash:
            return False

        if file_hash in self.processed_images:
            print(f"    ⏭️  処理済みのためスキップ")
            return False

        print(f"    🔄 埋め込み生成中...")
        embedding = self.get_image_embedding(image_path)
        if embedding is not None:
            new_data = {
                "filename": image_path.name,
                "filepath": str(image_path),
                "file_hash": file_hash,
                "file_size": image_path.stat().st_size,
                "embedding": embedding.tolist()
            }
            self.embeddings_data.append(new_data)
            self.processed_images.add(file_hash)
            self.save_embeddings()
            print(f"    ✅ 完了: {image_path.name}")
            return True
        else:
            print(f"    ❌ 失敗: {image_path.name}")
            return False

    def save_embeddings(self):
        """埋め込みデータをJSONファイルに保存"""
        try:
            with open(self.embeddings_file, 'w', encoding='utf-8') as f:
                json.dump(self.embeddings_data, f, ensure_ascii=False, indent=2)
            print(f"💾 データベース保存完了 (総数: {len(self.embeddings_data)}件)")
        except Exception as e:
            print(f"❌ 保存エラー: {e}")

class ImageChangeHandler(FileSystemEventHandler):
    def __init__(self, processor: ImageProcessor):
        self.processor = processor

    def on_created(self, event):
        """ファイルが作成されたときに呼ばれる"""
        if not event.is_directory:
            print(f"\n🆕 新規ファイル検出: {event.src_path}")
            self.processor.process_single_image(Path(event.src_path))
    
    def on_moved(self, event):
        """ファイルが移動/名前変更されたときに呼ばれる"""
        if not event.is_directory:
            print(f"\n🆕 ファイル移動/名前変更検出: {event.dest_path}")
            self.processor.process_single_image(Path(event.dest_path))


def run_batch_mode(processor: ImageProcessor):
    """バッチモードを実行する"""
    print("🚀 バッチモードで実行します...")
    processor.show_status()
    processor.process_new_images()
    print(f"\n✅ 全ての処理が完了しました!")
    print(f"▶️  検索を開始するには: python interactive_search.py")

def start_watching(processor: ImageProcessor, watch_path: str):
    """監視モードを開始する"""
    path = Path(watch_path)
    if not path.exists():
         print(f"❌ 監視対象のフォルダ '{watch_path}' が見つかりません。")
         sys.exit(1)

    print(f"👀 監視モードで実行します。対象フォルダ: '{watch_path}'")
    
    # 起動時にまず一括処理を実行し、未処理のファイルを処理する
    print("\n🔍 起動時スキャンを開始します...")
    processor.process_new_images()

    print("\n✅ 起動時スキャン完了。フォルダの監視を開始します...（Ctrl+Cで終了）")
    event_handler = ImageChangeHandler(processor)
    observer = Observer()
    observer.schedule(event_handler, watch_path, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        observer.stop()
        print("\n⏹️  監視を停止します。")
    observer.join()
    print("\n✅ 監視を終了しました。")


def main():
    """メインの実行関数（ディスパッチャー）"""
    print("🖼️  画像処理システム - RAGデータベース構築")
    print("=" * 50)
    
    parser = argparse.ArgumentParser(description="画像処理および監視システム")
    parser.add_argument(
        '--mode', 
        type=str, 
        default='batch', 
        choices=['batch', 'watch'],
        help="実行モードを選択: 'batch' (一括処理) or 'watch' (フォルダを監視)"
    )
    args = parser.parse_args()

    image_source_path = os.getenv("IMAGE_SOURCE_PATH", "images")
    processor = ImageProcessor(images_dir=image_source_path)
    
    # モードに応じて適切な関数を呼び出す
    if args.mode == 'batch':
        run_batch_mode(processor)
    elif args.mode == 'watch':
        start_watching(processor, image_source_path)


if __name__ == "__main__":
    main()