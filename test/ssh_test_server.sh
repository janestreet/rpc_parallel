#!/bin/bash
# NOTE: This file was written by AI.

set -uo pipefail

TEMP_DIR=""
SSH_PORT_FILE=""
SSHD_PID_FILE=""

find_free_port() {
  python3 -c "
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(('127.0.0.1', 0))
port = s.getsockname()[1]
s.close()
print(port)
"
}

cleanup() {
  if [[ -f "$SSHD_PID_FILE" ]]; then
    local pid
    pid=$(cat "$SSHD_PID_FILE" 2> /dev/null || true)
    if [[ -n "$pid" ]] && kill -0 "$pid" 2> /dev/null; then
      kill "$pid" 2> /dev/null || true
      local count=0
      while kill -0 "$pid" 2> /dev/null && [[ $count -lt 10 ]]; do
        sleep 0.1
        count=$((count + 1))
      done
    fi
  fi

  if [[ -n "$TEMP_DIR" ]] && [[ -d "$TEMP_DIR" ]]; then
    rm -rf "$TEMP_DIR"
  fi
}

start_server() {
  TEMP_DIR=$(mktemp -d -t ssh_test_server.XXXXXX)
  SSH_PORT_FILE="$TEMP_DIR/port"
  SSHD_PID_FILE="$TEMP_DIR/sshd.pid"

  ssh-keygen -t rsa -f "$TEMP_DIR/ssh_host_rsa_key" -N "" -q
  ssh-keygen -t ed25519 -f "$TEMP_DIR/ssh_host_ed25519_key" -N "" -q

  ssh-keygen -t rsa -f "$TEMP_DIR/id_rsa" -N "" -q

  mkdir -p "$TEMP_DIR/.ssh"
  cp "$TEMP_DIR/id_rsa.pub" "$TEMP_DIR/.ssh/authorized_keys"
  chmod 700 "$TEMP_DIR/.ssh"
  chmod 600 "$TEMP_DIR/.ssh/authorized_keys"

  local port
  port=$(find_free_port)
  echo "$port" > "$SSH_PORT_FILE"

  cat > "$TEMP_DIR/sshd_config" << EOF
Port $port
ListenAddress 0.0.0.0
ListenAddress ::
HostKey $TEMP_DIR/ssh_host_rsa_key
HostKey $TEMP_DIR/ssh_host_ed25519_key
PidFile $SSHD_PID_FILE
AuthorizedKeysFile $TEMP_DIR/.ssh/authorized_keys
PasswordAuthentication no
PubkeyAuthentication yes
ChallengeResponseAuthentication no
UsePAM no
PermitRootLogin no
StrictModes no
Subsystem sftp /usr/lib/openssh/sftp-server
AcceptEnv LANG LC_*
AcceptEnv RPC_PARALLEL_*
AcceptEnv OCAMLRUNPARAM
AcceptEnv TEST_*
AcceptEnv APP_*
AcceptEnv SHOULD_*
AcceptEnv SSH_TEST_SERVER_RUNNING
EOF

  /usr/sbin/sshd -f "$TEMP_DIR/sshd_config" -D > "$TEMP_DIR/sshd.log" 2>&1 &
  local sshd_pid=$!
  echo "$sshd_pid" > "$SSHD_PID_FILE"

  local count=0
  while ! nc -z 127.0.0.1 "$port" 2> /dev/null && [[ $count -lt 50 ]]; do
    sleep 0.1
    count=$((count + 1))
  done

  if ! nc -z 127.0.0.1 "$port" 2> /dev/null; then
    echo "Error: SSH server failed to start on port $port" >&2
    cleanup
    exit 1
  fi
}

stop_server() {
  cleanup
}

with_server() {
  start_server

  local port
  port=$(cat "$SSH_PORT_FILE")

  export SSH_TEST_SERVER_RUNNING=1
  export SSH_AUTH_SOCK=""

  cat > "$TEMP_DIR/ssh" << EOF
#!/bin/bash
# SSH wrapper that ensures we only connect to localhost or current host during tests
port="$port"
identity="$TEMP_DIR/id_rsa"
current_hostname="\$(hostname)"

found_host=false
hostname=""
new_args=()
skip_next=false

for arg in "\$@"; do
  if [[ "\$skip_next" == "true" ]]; then
    new_args+=("\$arg")
    skip_next=false
    continue
  fi

  case "\$arg" in
    -*)
      new_args+=("\$arg")
      if [[ "\$arg" =~ ^-(o|p|l|i|F|c|D|e|E|I|J|L|m|O|Q|R|S|W|w)$ ]]; then
        skip_next=true
      fi
      ;;
    *@*)
      hostname="\${arg#*@}"
      found_host=true
      new_args+=("\$arg")
      ;;
    *)
      if [[ "\$found_host" == "false" ]] && [[ ! "\$arg" =~ ^- ]]; then
        hostname="\$arg"
        found_host=true
      fi
      new_args+=("\$arg")
      ;;
  esac
done

if [[ "\$found_host" == "true" ]] && [[ "\$hostname" != "localhost" ]] && [[ "\$hostname" != "127.0.0.1" ]] && [[ "\$hostname" != "\$current_hostname" ]]; then
  echo "ERROR: SSH test wrapper called with unexpected host: \$hostname" >&2
  echo "Tests must only connect to localhost or the current host (\$current_hostname) when using the test SSH server" >&2
  exit 1
fi

extra_args=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o IdentityFile="\$identity" -o IdentitiesOnly=yes -o LogLevel=ERROR -p "\$port")

exec /usr/bin/ssh "\${extra_args[@]}" "\${new_args[@]}"
EOF
  chmod +x "$TEMP_DIR/ssh"

  cat > "$TEMP_DIR/scp" << EOF
#!/bin/bash
port="$port"
identity="$TEMP_DIR/id_rsa"
current_hostname="\$(hostname)"

for arg in "\$@"; do
  case "\$arg" in
    *:*)
      if [[ "\$arg" =~ ^([^@:]+@)?([^:]+): ]]; then
        hostname="\${BASH_REMATCH[2]}"
        if [[ "\$hostname" != "localhost" ]] && [[ "\$hostname" != "127.0.0.1" ]] && [[ "\$hostname" != "\$current_hostname" ]]; then
          echo "ERROR: SCP test wrapper called with unexpected host: \$hostname" >&2
          echo "Tests must only connect to localhost or the current host (\$current_hostname) when using the test SSH server" >&2
          exit 1
        fi
      fi
      ;;
  esac
done

extra_args=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o IdentityFile="\$identity" -o IdentitiesOnly=yes -o LogLevel=ERROR -P "\$port")

exec /usr/bin/scp "\${extra_args[@]}" "\$@"
EOF
  chmod +x "$TEMP_DIR/scp"

  export PATH="$TEMP_DIR:$PATH"

  "${@:2}"
  local exit_code=$?

  stop_server

  return $exit_code
}

case "${1:-}" in
  start)
    start_server
    ;;
  stop)
    stop_server
    ;;
  with)
    with_server "$@"
    ;;
  *)
    echo "Usage: $0 {start|stop|with <command>}" >&2
    exit 1
    ;;
esac
