#!/usr/bin/env bash
set -euo pipefail

# usage: ./scripts/gh-ci-errors.sh [owner/repo]
# dumps logs for all failed runs in the last 7 days

REPO_FLAG=""
if [[ -n "${1-}" ]]; then
  REPO_FLAG="--repo $1"
fi

RUN_IDS=$(
  gh run list $REPO_FLAG --status failure --limit 50 --json databaseId,createdAt \
    --jq '.[] | select(.createdAt > (now - 7*24*60*60 | todate)) | .databaseId'
)

if [[ -z "${RUN_IDS}" ]]; then
  echo "No failed runs in the last 7 days."
  exit 0
fi

mkdir -p ci-failures
JOBS="${JOBS:-}"
if [[ -z "${JOBS}" ]]; then
  JOBS="$(sysctl -n hw.ncpu 2>/dev/null || getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)"
fi

export REPO_FLAG
printf "%s\n" "${RUN_IDS}" | xargs -I{} -P "${JOBS}" bash -c '
  run_id="$1"
  [[ -z "${run_id}" ]] && exit 0
  echo "== run ${run_id} =="
  gh run view $REPO_FLAG "$run_id" --log-failed > "ci-failures/run-${run_id}.log" || true
' _ {}

echo "Wrote logs to ci-failures/"
