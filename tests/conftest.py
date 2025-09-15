import os
import sys

# Add project root and src to PYTHONPATH for tests
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
SRC_DIR = os.path.join(PROJECT_ROOT, "src")
if PROJECT_ROOT not in sys.path:
	sys.path.insert(0, PROJECT_ROOT)
if SRC_DIR not in sys.path:
	sys.path.insert(0, SRC_DIR) 