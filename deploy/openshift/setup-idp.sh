#!/usr/bin/env bash
# Configures the htpasswd identity provider and the jobprocessor-users
# group. Run ONCE by cluster-admin; replaces init-secrets.sh (API keys are
# gone — the cluster is the IdP, the API validates tokens via TokenReview).
# Idempotent: re-running updates passwords and group membership; the OAuth
# IdP entry is only added if absent.
set -euo pipefail

[ $# -ge 1 ] || { echo "usage: setup-idp.sh user:password [user:password ...]" >&2; exit 1; }
command -v htpasswd >/dev/null 2>&1 \
  || { echo "htpasswd not found (install httpd-tools / apache2-utils)" >&2; exit 1; }

IDP_NAME="jobprocessor-htpasswd"
SECRET_NAME="jobprocessor-htpasswd"
GROUP_NAME="jobprocessor-users"

HTPASSWD_FILE="$(mktemp)"
trap 'rm -f "$HTPASSWD_FILE"' EXIT

USERS=()
for pair in "$@"; do
  user="${pair%%:*}"
  pass="${pair#*:}"
  if [ -z "$user" ] || [ -z "$pass" ] || [ "$user" = "$pair" ]; then
    echo "bad user:password pair: $pair" >&2; exit 1
  fi
  htpasswd -B -b "$HTPASSWD_FILE" "$user" "$pass"
  USERS+=("$user")
done

oc create secret generic "$SECRET_NAME" -n openshift-config \
  --from-file=htpasswd="$HTPASSWD_FILE" \
  --dry-run=client -o yaml | oc apply -f -

if oc get oauth cluster -o jsonpath='{.spec.identityProviders[*].name}' \
    | grep -qw "$IDP_NAME"; then
  echo "IdP $IDP_NAME already configured on OAuth/cluster"
else
  IDP_JSON='{"name":"'"$IDP_NAME"'","mappingMethod":"claim","type":"HTPasswd","htpasswd":{"fileData":{"name":"'"$SECRET_NAME"'"}}}'
  # json-patch append fails when identityProviders is absent entirely; the
  # merge fallback then initializes the list (safe: the list was empty).
  oc patch oauth cluster --type=json \
    -p '[{"op":"add","path":"/spec/identityProviders/-","value":'"$IDP_JSON"'}]' \
  || oc patch oauth cluster --type=merge \
    -p '{"spec":{"identityProviders":['"$IDP_JSON"']}}'
fi

oc adm groups new "$GROUP_NAME" >/dev/null 2>&1 \
  || echo "group $GROUP_NAME already exists"
oc adm groups add-users "$GROUP_NAME" "${USERS[@]}"

echo
echo "Done. OAuth pods roll out in ~1 minute before logins work."
echo "Client flow: oc login --username <user> --password <pass>"
echo "             TOKEN=\$(oc whoami -t)"
echo "             curl -H \"Authorization: Bearer \$TOKEN\" https://<api-route>/jobs"
