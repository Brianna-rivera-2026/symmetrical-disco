#!/usr/bin/env bash
# Generates API user keys and creates the Kubernetes Secret the users-sync
# hook Job mounts. Idempotent: refuses to overwrite an existing secret so
# keys are never silently rotated. Keys are printed ONCE to stdout — hand
# them to the API consumers; they are not recoverable later (only hashes
# reach the database).
set -euo pipefail

NAMESPACE="${1:?usage: init-secrets.sh <namespace> [secret-name] [user ...]}"
SECRET_NAME="${2:-jobprocessor-api-user-keys}"
shift $(( $# > 2 ? 2 : $# ))
USERS=("${@:-alice bob}")
[ ${#USERS[@]} -eq 1 ] && read -ra USERS <<< "${USERS[0]}"

if oc get secret "$SECRET_NAME" -n "$NAMESPACE" >/dev/null 2>&1; then
  echo "secret $SECRET_NAME already exists in $NAMESPACE — leaving it untouched"
  exit 0
fi

TMP="$(mktemp)"
trap 'rm -f "$TMP"' EXIT

{
  printf '{'
  sep=""
  for user in "${USERS[@]}"; do
    key="$(openssl rand -hex 32)"
    printf '%s\n  "%s": "%s"' "$sep" "$user" "$key"
    sep=","
    echo "USER $user KEY $key" >&2
  done
  printf '\n}\n'
} > "$TMP"

oc create secret generic "$SECRET_NAME" -n "$NAMESPACE" \
  --from-file=api_user_keys.json="$TMP"
echo "created secret $SECRET_NAME in $NAMESPACE (raw keys printed above on stderr)"
