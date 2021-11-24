#!/bin/bash
export rpc_parallel_base=$(jenga root)/lib/rpc_parallel/public

function do_run() {
  local one_line=$1
  shift
  local exe=$1
  shift

  local args=$@
  local output_file=$(mktemp)
  # Redirect stderr to stdout so we can "-one-line" stderr
  $exe $args > $output_file 2>&1 &
  local pid=$!
  wait $pid

  # Putting "" around a variable preserves whitespace
  # not putting "" will replace new line characters with a single space
  if $one_line; then
    cat $output_file | tr -d '\n'
    echo ""
  else
    cat $output_file
  fi
  rm $output_file

  # Ensure all the workers shut down in some timely fashion
  local children=""
  for i in $(seq 50); do
    children=$(pgrep -f "/proc/$pid/exe")
    if test -z "$children"; then
      break
    fi
    sleep 0.1
  done

  if ! test -z "$children"; then
    echo "Didn't clean up worker processes"
  fi
}
export -f do_run

function run_one_line() {
  local exe=$rpc_parallel_base/$1
  shift

  do_run true $exe "$@"
}
export -f run_one_line

function run() {
  local exe=$rpc_parallel_base/$1
  shift

  do_run false $exe "$@"
}
export -f run

function run_absolute_path() {
  local exe=$1
  shift

  do_run false $exe "$@"
}
export -f run_absolute_path
