"""
画像処理専用スクリプト - RAGデータベース構築
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

load_dotenv()

class ImageProcessor:
    def __init__(self, images_dir: str = "images", embeddings_file: str = "embeddings.json"):
        self.images_dir = Path(images_dir)
        self.embeddings_file = embeddings_file
        self.api_key = os.getenv("COHERE_API_KEY")
        
        if not self.api_key:
            raise ValueError("COHERE_API_KEY not found in environment variables")
        
        self.client = cohere.ClientV2(api_key=self.api_key)
        self.processed_images: Set[str] = set()
        self.embeddings_data: List[Dict] = []
        
        # 既存データを読み込み
        self.load_existing_data()
    
    def load_existing_data(self):
        """既存の埋め込みデータを読み込み"""
        if os.path.exists(self.embeddings_file):
            try:
                with open(self.embeddings_file, 'r', encoding='utf-8') as f:
                    self.embeddings_data = json.load(f)
                
                # 処理済み画像のハッシュを記録
                for item in self.embeddings_data:
                    if 'file_hash' in item:
                        self.processed_images.add(item['file_hash'])
                
                print(f"📁 既存データ読み込み: {len(self.embeddings_data)}件")
                print(f"📝 処理済み画像: {len(self.processed_images)}件")
            except Exception as e:
                print(f"❌ 既存データ読み込みエラー: {e}")
                self.embeddings_data = []
        else:
            print("📂 新規データベースを作成します")
    
    def get_file_hash(self, file_path: Path) -> str:
        """ファイルのハッシュ値を計算（重複検出用）"""
        hash_sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_sha256.update(chunk)
        return hash_sha256.hexdigest()
    
    def image_to_base64_data_url(self, image_path: Path) -> str:
        """画像をbase64データURLに変換"""
        with open(image_path, "rb") as image_file:
            base64_bytes = base64.b64encode(image_file.read())
            base64_string = base64_bytes.decode('utf-8')
            
            ext = image_path.suffix.lower()
            mime_type = {
                '.jpg': 'image/jpeg',
                '.jpeg': 'image/jpeg', 
                '.png': 'image/png',
                '.webp': 'image/webp',
                '.gif': 'image/gif'
            }.get(ext, 'image/jpeg')
            
            return f"data:{mime_type};base64,{base64_string}"
    
    def get_image_embedding(self, image_path: Path) -> np.ndarray:
        """画像の埋め込みベクトルを生成"""
        try:
            base64_url = self.image_to_base64_data_url(image_path)
            
            image_input = {
                "content": [
                    {"type": "image_url", "image_url": {"url": base64_url}}
                ]
            }
            
            response = self.client.embed(
                model="embed-v4.0",
                inputs=[image_input],
                input_type="search_document",
                embedding_types=["float"]
            )
            
            embedding = response.embeddings.float_[0]
            return np.array(embedding)
            
        except Exception as e:
            print(f"❌ 埋め込み生成エラー ({image_path.name}): {e}")
            return None
    
    def process_new_images(self):
        """新しい画像のみを処理"""
        if not self.images_dir.exists():
            raise FileNotFoundError(f"画像ディレクトリ '{self.images_dir}' が見つかりません")
        
        image_extensions = {'.jpg', '.jpeg', '.png', '.webp', '.gif'}
        image_files = [f for f in self.images_dir.iterdir() 
                      if f.suffix.lower() in image_extensions]
        
        new_images = []
        skipped_images = []
        
        # 新しい画像と既処理画像を分類
        for image_path in image_files:
            file_hash = self.get_file_hash(image_path)
            if file_hash not in self.processed_images:
                new_images.append((image_path, file_hash))
            else:
                skipped_images.append(image_path.name)
        
        print(f"📊 画像分析結果:")
        print(f"  🆕 新規画像: {len(new_images)}件")
        print(f"  ⏭️  スキップ: {len(skipped_images)}件")
        
        if skipped_images:
            print(f"  📂 スキップされた画像: {', '.join(skipped_images)}")
        
        if not new_images:
            print("✅ 処理する新しい画像はありません")
            return
        
        # 新しい画像を処理
        processed_count = 0
        for i, (image_path, file_hash) in enumerate(new_images, 1):
            print(f"🔄 処理中 {i}/{len(new_images)}: {image_path.name}")
            
            embedding = self.get_image_embedding(image_path)
            if embedding is not None:
                # 新しいデータを追加
                new_data = {
                    "filename": image_path.name,
                    "filepath": str(image_path),
                    "file_hash": file_hash,
                    "file_size": image_path.stat().st_size,
                    "embedding": embedding.tolist()
                }
                
                self.embeddings_data.append(new_data)
                self.processed_images.add(file_hash)
                processed_count += 1
                
                print(f"✅ 完了: {image_path.name}")
            else:
                print(f"❌ 失敗: {image_path.name}")
        
        # データベースを保存
        if processed_count > 0:
            self.save_embeddings()
            print(f"\n🎉 処理完了! 新たに{processed_count}件の画像を追加しました")
            print(f"📊 総画像数: {len(self.embeddings_data)}件")
        
    def save_embeddings(self):
        """埋め込みデータをJSONファイルに保存"""
        try:
            with open(self.embeddings_file, 'w', encoding='utf-8') as f:
                json.dump(self.embeddings_data, f, ensure_ascii=False, indent=2)
            print(f"💾 データベース保存完了: {self.embeddings_file}")
        except Exception as e:
            print(f"❌ 保存エラー: {e}")
    
    def show_status(self):
        """現在の状況を表示"""
        print(f"\n📊 画像処理システム状況:")
        print(f"  📁 画像ディレクトリ: {self.images_dir}")
        print(f"  💾 データベースファイル: {self.embeddings_file}")
        print(f"  📈 処理済み画像数: {len(self.embeddings_data)}件")
        
        if self.embeddings_data:
            print(f"\n📂 処理済み画像一覧:")
            for item in self.embeddings_data:
                file_size_mb = item.get('file_size', 0) / (1024 * 1024)
                print(f"  - {item['filename']} ({file_size_mb:.2f}MB)")

def main():
    print("🖼️  画像処理システム - RAGデータベース構築")
    print("=" * 50)
    
    processor = ImageProcessor()
    
    # 現在の状況表示
    processor.show_status()
    
    # 新しい画像を処理
    print(f"\n🔄 新しい画像の処理を開始...")
    processor.process_new_images()
    
    print(f"\n✅ 画像処理完了!")
    print(f"検索を開始するには: python search.py")

if __name__ == "__main__":
    main()