#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [ -f .env ]; then
	set -a
	source .env  
	set +a
fi
if [ $# -eq 0 ]; then
	set -- -inicial data/test_case.json
fi
./bin/persistence -bind "tcp://*:5560" "$@"
