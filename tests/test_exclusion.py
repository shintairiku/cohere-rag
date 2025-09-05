#!/usr/bin/env python3
"""
Test script for exclusion list functionality

This script tests the image search API with exclusion list feature.
"""

import json
import requests
from typing import List, Dict

# API configuration
API_BASE_URL = "http://localhost:8000"  # Change this to your API URL
TEST_UUID = "test-uuid-123"  # Change this to a valid UUID in your system


def test_search_with_exclusion(
    uuid: str, 
    query: str = None, 
    trigger: str = "類似画像検索",
    exclude_files: List[str] = None
) -> Dict:
    """
    Test search API with exclusion list.
    
    Args:
        uuid: Company UUID
        query: Search query (optional for random search)
        trigger: Search type
        exclude_files: List of filenames to exclude
        
    Returns:
        API response as dictionary
    """
    url = f"{API_BASE_URL}/search"
    
    payload = {
        "uuid": uuid,
        "top_k": 5,
        "trigger": trigger,
        "exclude_files": exclude_files or []
    }
    
    if query:
        payload["q"] = query
    
    print(f"\n{'='*60}")
    print(f"Testing {trigger}")
    print(f"Query: {query if query else 'N/A'}")
    print(f"Exclude files: {exclude_files if exclude_files else 'None'}")
    print(f"{'='*60}")
    
    try:
        response = requests.post(url, json=payload)
        
        if response.status_code == 200:
            results = response.json()
            print(f"✅ Success! Got {len(results)} results")
            
            for i, result in enumerate(results, 1):
                print(f"\n  {i}. {result.get('filename', 'Unknown')}")
                if result.get('similarity') is not None:
                    print(f"     Similarity: {result['similarity']:.4f}")
                print(f"     Path: {result.get('filepath', 'N/A')}")
            
            return results
        else:
            print(f"❌ Error: Status {response.status_code}")
            print(f"   Details: {response.text}")
            return None
            
    except Exception as e:
        print(f"❌ Request failed: {e}")
        return None


def main():
    """Run test scenarios."""
    
    print("🧪 Testing Exclusion List Feature")
    print("=" * 70)
    
    # Test 1: Normal search without exclusion
    print("\n📝 Test 1: Normal search (no exclusion)")
    results1 = test_search_with_exclusion(
        uuid=TEST_UUID,
        query="製品画像",
        trigger="類似画像検索",
        exclude_files=[]
    )
    
    # Test 2: Search with exclusion list
    if results1 and len(results1) > 0:
        # Use the first result as an exclusion
        exclude_list = [results1[0].get('filename')]
        
        print("\n📝 Test 2: Search with exclusion list")
        results2 = test_search_with_exclusion(
            uuid=TEST_UUID,
            query="製品画像",
            trigger="類似画像検索",
            exclude_files=exclude_list
        )
        
        # Verify the excluded file is not in results
        if results2:
            excluded_file = exclude_list[0]
            found_excluded = any(r.get('filename') == excluded_file for r in results2)
            
            if not found_excluded:
                print(f"\n✅ Exclusion worked! '{excluded_file}' not in results")
            else:
                print(f"\n❌ Exclusion failed! '{excluded_file}' still in results")
    
    # Test 3: Random search without exclusion
    print("\n📝 Test 3: Random search (no exclusion)")
    results3 = test_search_with_exclusion(
        uuid=TEST_UUID,
        trigger="ランダム画像検索",
        exclude_files=[]
    )
    
    # Test 4: Random search with exclusion
    if results3 and len(results3) > 1:
        # Exclude first two results
        exclude_list = [r.get('filename') for r in results3[:2]]
        
        print("\n📝 Test 4: Random search with exclusion list")
        results4 = test_search_with_exclusion(
            uuid=TEST_UUID,
            trigger="ランダム画像検索",
            exclude_files=exclude_list
        )
        
        # Verify excluded files are not in results
        if results4:
            found_excluded = [f for f in exclude_list 
                            if any(r.get('filename') == f for r in results4)]
            
            if not found_excluded:
                print(f"\n✅ Exclusion worked! No excluded files in results")
            else:
                print(f"\n❌ Exclusion failed! Found: {found_excluded}")
    
    print("\n" + "=" * 70)
    print("🏁 Test completed!")


if __name__ == "__main__":
    main()