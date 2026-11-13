#!/bin/sh
set -eu
python "$(dirname "$0")/install.py" --bundle "$(dirname "$0")" --destination "$1"
