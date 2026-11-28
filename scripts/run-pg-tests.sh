#!/usr/bin/env bash
set -euo pipefail

compose=(docker compose --file compose.pg-tests.yml --project-name "zorm-pg-tests-$$")

cleanup() {
    status=$?
    trap - EXIT
    "${compose[@]}" logs --no-color postgres || true
    "${compose[@]}" down --volumes --remove-orphans || status=1
    printf '\nPostgreSQL test run exit status: %s\n' "$status"
    exit "$status"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

"${compose[@]}" up --detach --wait --wait-timeout 60 postgres
address=$("${compose[@]}" port postgres 5432)
export ZORM_POSTGRES_DSN="postgres://zorm:zorm@${address}/zorm_test?sslmode=disable"

go test -v -race -count=1 -timeout=5m -run '^TestPostgres_' ./...
