#!/usr/bin/env bash
# Java reads user.home from passwd, not from HOME. The Spark image entrypoint
# only adds a uid when /etc/passwd is writable, and it uses $SPARK_HOME as the
# home directory. Add the uid first, with home /tmp, so that entrypoint leaves
# the line alone and Ivy never sees "?".
set -euo pipefail

if ! getent passwd "$(id -u)" >/dev/null 2>&1; then
  if [[ -w /etc/passwd ]]; then
    echo "$(id -u):x:$(id -u):$(id -g):spark:/tmp:/bin/false" >> /etc/passwd
  else
    echo "Cannot add uid $(id -u) to /etc/passwd. Java user.home may be '?'." >&2
  fi
fi
export HOME="${HOME:-/tmp}"
exec /opt/entrypoint.sh "$@"
