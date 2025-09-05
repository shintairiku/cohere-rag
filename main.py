#!/usr/bin/env python3
"""
Main entry point for the Cohere RAG Image Search System

This file serves as a central entry point that can route to different modules
based on command-line arguments or can start the FastAPI server.

Usage:
    python main.py                          # Start FastAPI server
    python main.py --module vectorization   # Run vectorization
    python main.py --module scheduler       # Run scheduler
"""

import sys
import os
import argparse

# Add project root to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def start_api_server():
    """Start the FastAPI server."""
    import uvicorn
    from api.main import app
    
    port = int(os.environ.get("PORT", 8000))
    host = os.environ.get("HOST", "0.0.0.0")
    
    print(f"🚀 Starting Cohere RAG API server on {host}:{port}")
    uvicorn.run(app, host=host, port=port)

def run_vectorization():
    """Run vectorization module."""
    from vectorization import img_meta_processor_gdrive
    print("🧠 Running vectorization...")
    # Call the main function from img_meta_processor_gdrive
    img_meta_processor_gdrive.main()

def run_scheduler():
    """Run scheduler module."""
    from scheduler import scheduler
    print("⏰ Running scheduler...")
    scheduler.main()

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Cohere RAG Image Search System")
    parser.add_argument("--module", choices=["api", "vectorization", "scheduler"], 
                       default="api", help="Module to run (default: api)")
    
    args = parser.parse_args()
    
    try:
        if args.module == "api":
            start_api_server()
        elif args.module == "vectorization":
            run_vectorization()
        elif args.module == "scheduler":
            run_scheduler()
    except KeyboardInterrupt:
        print("\n⏹️  Shutting down...")
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()