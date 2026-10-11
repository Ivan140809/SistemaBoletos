#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [ -f .env ]; then
	set -a
	source .env  # carga DATABASE_URL sin dejar la contraseña en el código
	set +a
fi
if [ $# -eq 0 ]; then
	set -- -inicial data/test_case.json
fi
./bin/persistence -bind "tcp://*:5560" "$@"
