"""
画像処理専用スクリプト - RAGデータベース構築
"""

import os
import io
import base64
import json
import hashlib
from pathlib import Path
from typing import List, Dict, Set
from dotenv import load_dotenv
import cohere
import numpy as np
from PIL import Image

load_dotenv()

class ImageProcessor:
    def __init__(self, images_dir: str = "images", embeddings_file: str = "embeddings.json"):
        self.images_dir = Path(images_dir)
        self.embeddings_file = embeddings_file
        # 🆕 追加: Cohere APIの制限（20MB）
        self.max_file_size = 20 * 1024 * 1024  # 20MB in bytes
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
        # 🆕 変更: リサイズ処理を含む画像データ取得
        image_data = self.resize_image_if_needed(image_path)
         # 🆕 変更: 直接base64エンコード（ファイル読み込み不要）
        base64_string = base64.b64encode(image_data).decode('utf-8')
        """画像をbase64データURLに変換"""
            
        ext = image_path.suffix.lower()
        mime_type = {
            '.jpg': 'image/jpeg',
            '.jpeg': 'image/jpeg', 
            '.png': 'image/png',
            '.webp': 'image/webp',
            '.gif': 'image/gif'
        }.get(ext, 'image/jpeg')
        
        return f"data:{mime_type};base64,{base64_string}"
    
    def resize_image_if_needed(self, image_path: Path) -> bytes:
        """
        画像のファイルサイズが20MBを超える場合、リサイズして制限内に収める
        :param image_path: 画像ファイルのパス
        :return: リサイズされた画像のバイトデータ
        """
        file_size = image_path.stat().st_size
        
        # ファイルサイズが制限内の場合はそのまま返す
        if file_size <= self.max_file_size:
            with open(image_path, 'rb') as f:
                return f.read()
        
        print(f"📏 大きなファイルを検出 ({file_size / (1024*1024):.1f}MB): {image_path.name}")
        print(f"   🔄 リサイズを実行中...")
        
        try:
            with Image.open(image_path) as img:
                # 元の画像情報を保持
                original_format = img.format
                original_size = img.size
                
                # RGBA画像の場合はRGBに変換（JPEGサポートのため）
                if img.mode in ('RGBA', 'LA', 'P'):
                    # 透明背景を白に変換
                    background = Image.new('RGB', img.size, (255, 255, 255))
                    if img.mode == 'P':
                        img = img.convert('RGBA')
                    background.paste(img, mask=img.split()[-1] if img.mode in ('RGBA', 'LA') else None)
                    img = background
                
                # 品質を段階的に下げながらリサイズを試行
                quality_levels = [95, 85, 75, 65, 55, 45]
                scale_factors = [0.9, 0.8, 0.7, 0.6, 0.5, 0.4]
                
                for quality in quality_levels:
                    for scale in scale_factors:
                        # 新しいサイズを計算
                        new_width = int(original_size[0] * scale)
                        new_height = int(original_size[1] * scale)
                        
                        # リサイズ
                        resized_img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)
                        
                        # バイトデータに変換
                        output = io.BytesIO()
                        
                        # フォーマットを決定（JPEGを優先して圧縮効率を高める）
                        save_format = 'JPEG' if original_format in ['JPEG', 'JPG'] or quality < 85 else original_format or 'JPEG'
                        
                        if save_format == 'JPEG':
                            resized_img.save(output, format=save_format, quality=quality, optimize=True)
                        else:
                            resized_img.save(output, format=save_format, optimize=True)
                        
                        resized_data = output.getvalue()
                        resized_size = len(resized_data)
                        
                        # サイズが制限内に収まった場合
                        if resized_size <= self.max_file_size:
                            compression_ratio = (file_size - resized_size) / file_size * 100
                            print(f"   ✅ リサイズ完了: {original_size} → {new_width}x{new_height}")
                            print(f"   📉 サイズ削減: {file_size/(1024*1024):.1f}MB → {resized_size/(1024*1024):.1f}MB ({compression_ratio:.1f}%削減)")
                            print(f"   🎯 品質: {quality}%, フォーマット: {save_format}")
                            return resized_data
                
                # どの設定でも制限内に収まらない場合の最終手段
                print(f"   ⚠️  警告: 最大圧縮でも制限を超過、最小サイズで処理")
                final_size = (400, 300)  # 最小サイズ
                resized_img = img.resize(final_size, Image.Resampling.LANCZOS)
                output = io.BytesIO()
                resized_img.save(output, format='JPEG', quality=45, optimize=True)
                return output.getvalue()
                
        except Exception as e:
            print(f"   ❌ リサイズエラー: {e}")
            print(f"   📄 元ファイルをそのまま使用（APIエラーの可能性あり）")
            with open(image_path, 'rb') as f:
                return f.read()
    
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

    def get_meta_embedding(self, image_path: Path) -> np.ndarray:
        # ファイルパスを検索クエリ化
        # 例: images/details/sample.png → 'images details sample.png'
        path_parts = list(image_path.parts)
        if len(path_parts) > 1:
            # 拡張子付きファイル名はそのまま
            query_str = ' '.join(path_parts[:-1] + [image_path.name])
        else:
            query_str = image_path.name
        # テキストベクトル（検索クエリとして）
        try:
            response = self.client.embed(
                model="embed-v4.0",
                texts=[query_str],
                input_type="search_query",
                embedding_types=["float"]
            )
            meta_vec = np.array(response.embeddings.float_[0])
            return meta_vec
        except Exception as e:
            print(f"❌ メタデータベクトル生成失敗: {image_path} ({e})")
            return None
        
    def get_weighted_image_and_meta_embedding(self, image_path: Path, w: float = 0.5) -> np.ndarray:
        """
        画像のみのベクトル(img_vec)と、ファイルパスを検索クエリ化したテキストベクトル(meta_vec)を生成し、
        重みwで合成した最終ベクトルを返す。
        :param image_path: 画像ファイルのパス
        :param w: メタデータ（テキスト）ベクトルの重み（0.0〜1.0）
        :return: 合成ベクトル（np.ndarray）
        """
        # 画像のみのベクトル
        img_vec = self.get_image_embedding(image_path)
        if img_vec is None:
            print(f"❌ 画像ベクトル生成失敗: {image_path}")
            return None
        # ファイルパスを検索クエリ化
        # 例: images/details/sample.png → 'images details sample.png'
        meta_vec = self.get_meta_embedding(image_path)
        # 合成
        w = np.dot(img_vec, meta_vec) / (np.linalg.norm(img_vec) * np.linalg.norm(meta_vec))
        vec = w * meta_vec + (1.0 - w) * img_vec
        return vec
    
    def process_new_images(self):
        """新しい画像のみを処理"""
        if not self.images_dir.exists():
            raise FileNotFoundError(f"画像ディレクトリ '{self.images_dir}' が見つかりません")
        
        image_extensions = {'.jpg', '.jpeg', '.png', '.webp', '.gif'}
        image_files = [f for f in self.images_dir.iterdir() 
                      if f.suffix.lower() in image_extensions]
        
        new_images = []
        skipped_images = []
        large_images = []  # 🆕 追加: 大容量画像リスト
        
        # 新しい画像と既処理画像を分類
        for image_path in image_files:
            file_hash = self.get_file_hash(image_path)
            if file_hash not in self.processed_images:
                new_images.append((image_path, file_hash))
                # 🆕 追加: 大きなファイルをカウント
                if image_path.stat().st_size > self.max_file_size:
                    large_images.append(image_path)
            else:
                skipped_images.append(image_path.name)
        
        print(f"📊 画像分析結果:")
        print(f"  🆕 新規画像: {len(new_images)}件")
        print(f"  📏 大容量画像（リサイズ対象）: {len(large_images)}件")  # 🆕 追加
        print(f"  ⏭️  スキップ: {len(skipped_images)}件")
        
        if skipped_images:
            print(f"  📂 スキップされた画像: {', '.join(skipped_images)}")
        
        # 🆕 追加: 大容量画像の詳細表示
        if large_images:
            print(f"  🔄 リサイズ予定の画像:")
            for img_path in large_images:
                size_mb = img_path.stat().st_size / (1024 * 1024)
                print(f"    - {img_path.name} ({size_mb:.1f}MB)")

        if not new_images:
            print("✅ 処理する新しい画像はありません")
            return
        
        # 新しい画像を処理
        processed_count = 0
        for i, (image_path, file_hash) in enumerate(new_images, 1):
            print(f"🔄 処理中 {i}/{len(new_images)}: {image_path.name}")
            
            # embedding = self.get_image_embedding(image_path)
            # embedding = self.get_image_and_path_embedding(image_path)
            embedding = self.get_weighted_image_and_meta_embedding(image_path)
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
        print(f"  📏 ファイルサイズ上限: {self.max_file_size / (1024*1024):.0f}MB")  # 🆕 追加
        
        if self.embeddings_data:
            print(f"\n📂 処理済み画像一覧:")
            for item in self.embeddings_data:
                file_size_mb = item.get('file_size', 0) / (1024 * 1024)
                print(f"  - {item['filename']} ({file_size_mb:.2f}MB)")

def main():
    print("🖼️  画像処理システム - RAGデータベース構築")
    print("=" * 50)
    
    processor = ImageProcessor(
        images_dir="images/high_resolution", embeddings_file="embedding_dynamic_weight_high_resolution.json"
        )
    
    # 現在の状況表示
    processor.show_status()
    
    # 新しい画像を処理
    print(f"\n🔄 新しい画像の処理を開始...")
    processor.process_new_images()
    
    print(f"\n✅ 画像処理完了!")
    print(f"検索を開始するには: python search.py")

if __name__ == "__main__":
    main()