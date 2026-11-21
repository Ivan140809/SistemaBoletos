#!/usr/bin/env bash
# maquina 3, persistencia principal y la unica que reinicia la base. uso ./scripts/machine3_storage.sh -inicial data/test_case.json -workers 8
# las REPLICAS de persistencia en otras maquinas se arrancan con scripts/persistence_extra.sh
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
