import os
import sys

# Make the shared ``_helpers`` module importable from tests in subdirectories
# (tests/pkcs11, tests/windows) under pytest's default "prepend" import mode.
sys.path.insert(0, os.path.dirname(__file__))
