#!/usr/bin/env python3
"""
Social Sources Only Test
Runs only the social sources to test Reddit implementation.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

# Add current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def run_social_sources():
    """Run only social sources with proper configuration."""
    print("Running Social Sources Only Test...")
    print("=" * 50)
    
    # Create test output directory
    test_output_dir = "test_social_output"
    Path(test_output_dir).mkdir(exist_ok=True)
    
    # Command to run pipeline with only social sources
    cmd = [
        sys.executable, "src/jobs/pipeline.py",
        "--only-sources", "social_reddit",
        "--output-dir", test_output_dir,
        "--timeout", "30",
        "--retries", "2", 
        "--backoff", "1.5",
        "--max-workers", "1",
        "--social-enabled",
        "--quiet"
    ]
    
    print(f"Running command: {' '.join(cmd)}")
    print("This may take a few minutes...")
    
    try:
        # Run the pipeline
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120  # 2 minute timeout
        )
        
        print(f"Return code: {result.returncode}")
        
        if result.stdout:
            print("STDOUT:")
            print(result.stdout)
        
        if result.stderr:
            print("STDERR:")
            print(result.stderr)
        
        # Check if output files were created
        output_files = list(Path(test_output_dir).glob("*"))
        print(f"\nOutput files created: {len(output_files)}")
        
        for file_path in output_files:
            if file_path.is_file():
                print(f"  {file_path.name}: {file_path.stat().st_size} bytes")
        
        # Try to read the fetch report if it exists
        fetch_report_path = Path(test_output_dir) / "jobs-fetch-report.json"
        if fetch_report_path.exists():
            print(f"\nReading fetch report: {fetch_report_path}")
            with open(fetch_report_path) as f:
                report = json.load(f)
            
            print("Report summary:")
            print(f"  Sources: {len(report.get('sources', []))}")
            print(f"  Total jobs: {report.get('summary', {}).get('outputCount', 0)}")
            
            # Check social reddit details
            for source in report.get('sources', []):
                if source.get('name') == 'social_reddit':
                    print(f"  Social Reddit status: {source.get('status')}")
                    print(f"  Jobs fetched: {source.get('fetchedCount', 0)}")
                    print(f"  Jobs kept: {source.get('keptCount', 0)}")
                    
        return result.returncode == 0
        
    except subprocess.TimeoutExpired:
        print("❌ Command timed out after 2 minutes")
        return False
    except Exception as e:
        print(f"❌ Error running command: {e}")
        return False

if __name__ == "__main__":
    success = run_social_sources()
    print(f"\n{'✅ Test completed successfully!' if success else '❌ Test failed!'}")
    sys.exit(0 if success else 1)