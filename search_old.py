"""
画像検索専用スクリプト - 自然言語クエリによる検索
"""

import os
import json
import numpy as np
from typing import List, Dict
from dotenv import load_dotenv
import cohere

load_dotenv()

class ImageSearcher:
    def __init__(self, embeddings_file: str = "embedding_gdrive_shoken.json"):
        self.embeddings_file = embeddings_file
        self.api_key = os.getenv("COHERE_API_KEY")
        
        if not self.api_key:
            raise ValueError("COHERE_API_KEY not found in environment variables")
        
        self.client = cohere.ClientV2(api_key=self.api_key)
        self.embeddings_data = []
        
        # 埋め込みデータを読み込み
        self.load_embeddings()
    
    def load_embeddings(self):
        """埋め込みデータを読み込み"""
        if not os.path.exists(self.embeddings_file):
            raise FileNotFoundError(
                f"埋め込みファイル '{self.embeddings_file}' が見つかりません。\n"
                f"まず 'python image_processor.py' を実行してください。"
            )
        
        try:
            with open(self.embeddings_file, 'r', encoding='utf-8') as f:
                self.embeddings_data = json.load(f)
            print(f"📁 {len(self.embeddings_data)}件の画像データを読み込みました")
        except Exception as e:
            raise Exception(f"データ読み込みエラー: {e}")
    
    def get_text_embedding(self, text: str) -> np.ndarray:
        """テキストクエリの埋め込みベクトルを生成"""
        try:
            response = self.client.embed(
                model="embed-v4.0",
                texts=[text],
                input_type="search_query",
                embedding_types=["float"]
            )
            
            embedding = response.embeddings.float_[0]
            return np.array(embedding)
            
        except Exception as e:
            print(f"❌ テキスト埋め込み生成エラー: {e}")
            return None
    
    def cosine_similarity(self, a: np.ndarray, b: np.ndarray) -> float:
        """コサイン類似度を計算"""
        return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))
    
    def search_images(self, query: str, top_k: int = 5) -> List[Dict]:
        """自然言語クエリで画像を検索"""
        if not self.embeddings_data:
            print("❌ 埋め込みデータがありません")
            return []
        
        print(f"🔍 検索クエリ: '{query}'")
        
        # クエリの埋め込みベクトルを生成
        query_embedding = self.get_text_embedding(query)
        if query_embedding is None:
            print("❌ クエリの埋め込み生成に失敗")
            return []
        
        # 全画像との類似度を計算
        similarities = []
        for item in self.embeddings_data:
            image_embedding = np.array(item["embedding"])
            similarity = self.cosine_similarity(query_embedding, image_embedding)
            
            similarities.append({
                "filename": item["filename"],
                "filepath": item["filepath"],
                "similarity": similarity,
                "file_size_mb": item.get("file_size", 0) / (1024 * 1024)
            })
        
        # 類似度でソート（降順）
        similarities.sort(key=lambda x: x["similarity"], reverse=True)
        
        return similarities[:top_k]
    
    def print_search_results(self, results: List[Dict]):
        """検索結果をフォーマットして表示"""
        if not results:
            print("❌ 検索結果がありません")
            return
        
        print(f"\n🎯 検索結果 (上位{len(results)}件):")
        print("=" * 60)
        
        for i, result in enumerate(results, 1):
            similarity_bar = "█" * int(result["similarity"] * 20)
            print(f"{i}. 📁 {result['filename']}")
            print(f"   📊 類似度: {result['similarity']:.4f} {similarity_bar}")
            print(f"   📂 パス: {result['filepath']}")
            print(f"   💾 サイズ: {result['file_size_mb']:.2f}MB")
            print()
    
    def search_interactive(self):
        """対話型検索インターフェース"""
        print(f"\n🔍 対話型画像検索システム")
        print("=" * 50)
        print(f"💡 使用可能な画像: {len(self.embeddings_data)}件")
        print(f"💡 例: 'モダンなリビング', '人物の写真', '医療関連画像'")
        print(f"💡 終了: 'quit', 'exit', 'q' を入力")
        
        while True:
            try:
                query = input(f"\n🔍 検索クエリを入力: ").strip()
                
                if query.lower() in ['quit', 'exit', 'q']:
                    print("👋 検索を終了します")
                    break
                
                if not query:
                    continue
                
                results = self.search_images(query, top_k=5)
                self.print_search_results(results)
                
            except KeyboardInterrupt:
                print(f"\n👋 検索を終了します")
                break
            except EOFError:
                print(f"\n👋 検索を終了します")
                break
            except Exception as e:
                print(f"❌ エラー: {e}")
    
    def search_single_query(self, query: str, top_k: int = 5):
        """単一クエリ検索（テスト用）"""
        print(f"🔍 検索実行: '{query}'")
        results = self.search_images(query, top_k=top_k)
        self.print_search_results(results)
        return results
    
    def search_batch(self, queries: List[str], top_k: int = 3):
        """複数クエリの一括検索"""
        print(f"\n📊 一括検索実行 ({len(queries)}件のクエリ)")
        print("=" * 60)
        
        for i, query in enumerate(queries, 1):
            print(f"\n📝 クエリ {i}: {query}")
            print("-" * 40)
            
            results = self.search_images(query, top_k=top_k)
            self.print_search_results(results)
    
    def show_available_images(self):
        """利用可能な画像一覧を表示"""
        print(f"\n📂 利用可能な画像一覧 ({len(self.embeddings_data)}件):")
        print("=" * 60)
        
        for i, item in enumerate(self.embeddings_data, 1):
            file_size_mb = item.get("file_size", 0) / (1024 * 1024)
            print(f"{i}. {item['filename']} ({file_size_mb:.2f}MB)")

def main():
    print("🔍 画像検索システム")
    print("=" * 50)
    
    try:
        searcher = ImageSearcher()
        
        # 利用可能な画像を表示
        searcher.show_available_images()
        
        # 対話型検索を開始
        searcher.search_interactive()
        
    except FileNotFoundError as e:
        print(f"❌ {e}")
    except Exception as e:
        print(f"❌ システムエラー: {e}")

if __name__ == "__main__":
    main()