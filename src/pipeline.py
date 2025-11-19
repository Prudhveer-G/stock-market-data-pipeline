"""
pipeline.py

Simple runner that executes the stock pipeline end-to-end:
1) fetch_data.py -> downloads raw CSVs into data/
2) process_data.py -> cleans + feature engineers
3) train_model.py -> trains model and writes predictions to output/
Usage: python src/pipeline.py
"""

import subprocess
import sys
import os

SCRIPTS = ["fetch_data.py", "process_data.py", "train_model.py"]
SRC_DIR = os.path.dirname(__file__)

def run_script(script_name):
    script_path = os.path.join(SRC_DIR, script_name)
    print(f"\n--- Running: {script_name} ---")
    res = subprocess.run([sys.executable, script_path], capture_output=False)
    if res.returncode != 0:
        raise SystemExit(f"Script {script_name} failed with return code {res.returncode}")

def main():
    for s in SCRIPTS:
        run_script(s)
    print("\nPipeline finished successfully. Check data/ and output/ for results.")

if __name__ == "__main__":
    main()
