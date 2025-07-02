#!/usr/bin/env python3
"""
対話型画像検索スクリプト
"""

import sys
from search import ImageSearcher

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
        print("💡 先に 'python image_processor.py' を実行してください")
        sys.exit(1)
    except Exception as e:
        print(f"❌ システムエラー: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()