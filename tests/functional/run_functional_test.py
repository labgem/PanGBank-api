#!/usr/bin/env python3
"""
Standalone CLI script for running pangbank_db functional tests.

This script allows you to:
1. Run a full end-to-end functional test
2. Generate test data files only (for inspection or manual testing)
3. Test with existing data files
4. Keep test data for debugging

Usage:
    # Run full functional test (creates temp data, runs test, cleans up)
    python tests/functional/run_functional_test.py
    
    # Keep test data after test for inspection
    python tests/functional/run_functional_test.py --keep-data
    
    # Generate test data only (no database test)
    python tests/functional/run_functional_test.py --generate-only --output ./test_data
    
    # Test with existing data and database
    python tests/functional/run_functional_test.py --data-dir ./test_data --db-path ./test.db
    
    # Test with custom JSON file
    python tests/functional/run_functional_test.py --data-dir ./test_data --json-file ./custom_config.json
    
    # Use pangbank_db CLI directly with generated data
    python tests/functional/run_functional_test.py --generate-only --output ./mytest
    export PANGBANK_DB_PATH=./test.db
    export PANGBANK_DATA_DIR=./mytest
    pangbank_db add-collection-release ./mytest/collection_release_info.json
"""

from __future__ import annotations

import argparse
import sys
import tempfile
from pathlib import Path

# Add the parent directory to the path to import the test module
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from tests.functional.test_functional_pangbank_db import (
    setup_functional_test_data,
    verify_database_content,
)


def generate_test_data(output_dir: Path) -> Path:
    """Generate test data files in the specified directory."""
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Generating test data in: {output_dir}")
    json_file = setup_functional_test_data(output_dir)
    
    print("\n[SUCCESS] Test data generated successfully!")
    print(f"\nGenerated files:")
    print(f"  - Configuration: {json_file}")
    print(f"  - Taxonomy: {output_dir / 'taxonomy.tsv'}")
    print(f"  - Metadata: {output_dir / 'metadata.tsv'}")
    print(f"  - Genomes: {output_dir / 'genomes/'}")
    print(f"  - Pangenomes: {output_dir / 'pangenomes/'}")
    print(f"  - Status files: {output_dir / 'status/'}")
    
    print(f"\nTo test with pangbank_db CLI:")
    print(f"  export PANGBANK_DB_PATH=./my_test.db")
    print(f"  export PANGBANK_DATA_DIR={output_dir}")
    print(f"  pangbank_db add-collection-release {json_file}")
    
    return json_file


def run_functional_test_with_data(json_file: Path, data_dir: Path, db_path: Path | None = None) -> int:
    """Run functional test using existing data directory and JSON configuration.
    
    Args:
        json_file: Path to the collection release JSON configuration file
        data_dir: Path to the data directory containing test data
        db_path: Optional path to the database file (creates temporary if None)
    
    Returns:
        0 if test passes, 1 if test fails
    """
    import os
    import sys
    
    if not json_file.exists():
        print(f"[ERROR] {json_file} not found!")
        return 1
    
    # Use provided db_path or create a temporary one
    if db_path is None:
        temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        db_path = Path(temp_db.name)
        temp_db.close()
        cleanup_db = True
    else:
        cleanup_db = False
    
    print(f"Using database: {db_path}")
    print(f"Using data directory: {data_dir}")
    
    # Ensure parent directory exists
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Override database path using environment variable
    original_db_env = os.environ.get("PANGBANK_DB_PATH")
    os.environ["PANGBANK_DB_PATH"] = str(db_path)
    
    try:
        # Clear import cache to reload config with new env var
        if "pangbank_api.config" in sys.modules:
            del sys.modules["pangbank_api.config"]
        if "pangbank_api.database" in sys.modules:
            del sys.modules["pangbank_api.database"]
        if "pangbank_api.manage_db.pangbank_db" in sys.modules:
            del sys.modules["pangbank_api.manage_db.pangbank_db"]
        
        from pangbank_api.manage_db.pangbank_db import add_collection_release
        
        print("\n" + "=" * 70)
        print("Running pangbank_db add-collection-release...")
        print("=" * 70 + "\n")
        
        # Run the command
        add_collection_release(
            collection_release_json=json_file,
            pangbank_data_dir=data_dir,
        )
        
        print("\n" + "=" * 70)
        print("Verifying database content...")
        print("=" * 70 + "\n")
        
        # Verify results
        results = verify_database_content(db_path)
        
        if results["success"]:
            print("[SUCCESS] All verifications passed!")
            print(f"\nDatabase statistics:")
            print(f"  - Collections: {results['collections_count']}")
            print(f"  - Releases: {results['releases_count']}")
            print(f"  - Genomes: {results['genomes_count']}")
            print(f"  - Pangenomes: {results['pangenomes_count']}")
            return 0
        else:
            print(f"[ERROR] Verification failed: {results.get('error', 'Unknown error')}")
            return 1
    
    except Exception as e:
        print(f"\n[ERROR] Error during test: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    finally:
        # Restore original environment
        if original_db_env is not None:
            os.environ["PANGBANK_DB_PATH"] = original_db_env
        elif "PANGBANK_DB_PATH" in os.environ:
            del os.environ["PANGBANK_DB_PATH"]
        
        if cleanup_db and db_path.exists():
            db_path.unlink()


def main():
    parser = argparse.ArgumentParser(
        description="Run pangbank_db functional tests",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    
    parser.add_argument(
        "--generate-only",
        action="store_true",
        help="Only generate test data files, don't run the test",
    )
    
    parser.add_argument(
        "--output", "-o",
        type=Path,
        help="Output directory for test data (used with --generate-only)",
    )
    
    parser.add_argument(
        "--data-dir",
        type=Path,
        help="Use existing data directory instead of generating new data",
    )
    
    parser.add_argument(
        "--json-file",
        type=Path,
        help="Path to collection release JSON file (default: <data-dir>/collection_release_info.json)",
    )
    
    parser.add_argument(
        "--db-path",
        type=Path,
        help="Database path to use (default: temporary database)",
    )
    
    parser.add_argument(
        "--keep-data",
        action="store_true",
        help="Keep generated test data after test completes",
    )
    
    args = parser.parse_args()
    
    # Generate-only mode
    if args.generate_only:
        output_dir = args.output or Path("./functional_test_data")
        generate_test_data(output_dir)
        return 0
    
    # Test with existing data
    if args.data_dir:
        if not args.data_dir.exists():
            print(f"[ERROR] Data directory {args.data_dir} does not exist")
            return 1
        
        json_file = args.json_file or args.data_dir / "collection_release_info.json"
        return run_functional_test_with_data(json_file, args.data_dir, args.db_path)
    
    # Full test with temporary data
    if args.keep_data:
        # Create persistent directory
        test_data_dir = Path("./functional_test_data")
        test_data_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"Generating test data in: {test_data_dir}")
        json_file = setup_functional_test_data(test_data_dir)
        
        db_path = args.db_path or Path("./functional_test.db")
        
        result = run_functional_test_with_data(json_file, test_data_dir, db_path)
        
        print(f"\nTest data preserved in: {test_data_dir}")
        print(f"Database saved at: {db_path}")
        
        return result
    else:
        # Use temporary directory
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            test_data_dir = tmp_path / "test_data"
            test_data_dir.mkdir(parents=True, exist_ok=True)
            
            print(f"Generating test data in temporary directory...")
            json_file = setup_functional_test_data(test_data_dir)
            
            db_path = args.db_path or tmp_path / "test.db"
            
            return run_functional_test_with_data(json_file, test_data_dir, db_path)


if __name__ == "__main__":
    sys.exit(main())
