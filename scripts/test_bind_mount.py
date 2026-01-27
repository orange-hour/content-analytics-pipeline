"""
Quick test script to verify bind mounts are working
Run this to confirm you can edit code locally and run it in the container
"""
import os
from datetime import datetime

def main():
    """Test function to verify bind mounts"""
    print("=" * 70)
    print("BIND MOUNT TEST")
    print("=" * 70)
    print(f"Script executed at: {datetime.now()}")
    print(f"Running from: {os.getcwd()}")
    print(f"Python version: {os.sys.version}")
    print("=" * 70)
    print("✅ SUCCESS! Bind mounts are working!")
    print("=" * 70)
    print("\nHow to test:")
    print("1. Edit this file locally (add a print statement below)")
    print("2. Run: docker-compose exec airflow-webserver python /opt/airflow/scripts/test_bind_mount.py")
    print("3. See your changes immediately - no rebuild needed!")
    print("=" * 70)

    # ADD YOUR OWN PRINT STATEMENTS HERE TO TEST!
    # Example: print("Hello from my local machine!")


if __name__ == "__main__":
    main()
