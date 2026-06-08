import os
import sys

# Make the repo-root scripts (download.py, upload.py) importable from tests.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
