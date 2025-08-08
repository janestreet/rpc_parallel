


# NOTE:
# 
# if this test fails with the error "Permission denied
# (publickey,gssapi-keyex,gssapi-with-mic,password)", you should run the
# following:
# 
#   $ cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
#   $ chmod 600 ~/.ssh/authorized_keys

  $ $TESTDIR/ssh_test_server.sh with run test/copy_executable.exe -dir $(pwd) -worker $(hostname)
  Ok

  $ run example/serve.exe
  Success.

  $ $TESTDIR/ssh_test_server.sh with run test/wrap_test.exe -wrapper $TESTDIR/print_and_run.sh -host localhost
  Worker successfully started
  [WORKER STDOUT]: Ran with print_and_run!

  $ run test/env_test.exe basic-test
  WORKER: TEST_ENV_KEY=potentially "problematic" \"test\" string ()!
  WORKER: SHOULD_NOT_EXIST=<none>

  $ run test/env_test.exe special-var
  WORKER: OCAMLRUNPARAM=foo=bar
  WORKER: OCAMLRUNPARAM=foo=user-supplied

  $ $TESTDIR/ssh_test_server.sh with run test/env_test.exe special-var -host localhost
  WORKER: OCAMLRUNPARAM=foo=bar
  WORKER: OCAMLRUNPARAM=foo=user-supplied

  $ run example/alternative_init.exe main
  Success.

  $ run_one_line test/raise_on_connection_state_init.exe
  expected failure:*text of expected failure* (glob)

  $ run_one_line test/run_exn.exe
  expected failure:*text of expected failure* (glob)

  $ run example/number_stats.exe -nblocks 100
  Samples: * (glob)
  Mean: * (glob)
  Variance: * (glob)

  $ run example/add_numbers.exe -max 100 -ntimes 10
  0: 4950
  1: 4950
  2: 4950
  3: 4950
  4: 4950
  5: 4950
  6: 4950
  7: 4950
  8: 4950
  9: 4950

  $ run example/rpc_direct_pipe.exe -max 10 -delay 0.01s
  Sum_worker.sum: 0
  Sum_worker.sum: 1
  Sum_worker.sum: 3
  Sum_worker.sum: 6
  Sum_worker.sum: 10
  Sum_worker.sum: 15
  Sum_worker.sum: 21
  Sum_worker.sum: 28
  Sum_worker.sum: 36
  Sum_worker.sum: 45

  $ run example/abort_direct_pipe.exe
  Ping: 0
  Pong: 0
  Ping: 1
  Pong: 1
  Ping: 2
  Pong: 2
  Ping: 3
  Pong: 3
  Ping: 4
  Pong: 4
  Closed: By_remote_side
  Worker reports pongs closed

  $ run example/async_log.exe
  (V2
   ((time (*)) (level (Info)) (glob)
    (message (String "worker log message")) (tags ())))
  (V2
   ((time (*)) (level (Info)) (glob)
    (message (String "worker log message")) (tags ())))
  (V2
   ((time (*)) (level (Info)) (glob)
    (message (String "worker log message")) (tags ())))

  $ run example/stream_workers.exe -num-elts 10
  Ok.

  $ run test/fd.exe
  Ok

  $ run test/krb_expert.exe main
  Success.

  $ run example/spawn_in_foreground.exe
  [WORKER STDERR]: *-*-* *:*:*.* Info ("Rpc_parallel: initial client connection closed... Shutting down."(reason* (glob)
  [WORKER STDOUT]: HELLO
  [WORKER STDOUT]: HELLO2

  $ run test/spawn_in_foreground_zombie.exe
  "No zombies"
  "No zombies"

# Treat this test specially because it removes the exe momentarily. This causes
# jenga to loop (rebuilding the exe then rerunning the test)
  $ cp $rpc_parallel_base/test/remove_running_executable.exe /tmp

  $ run_absolute_path /tmp/remove_running_executable.exe -spawn-local
  Ok.

  $ $TESTDIR/ssh_test_server.sh with run_absolute_path /tmp/remove_running_executable.exe -spawn-remote
  Ok.

  $ rm /tmp/remove_running_executable.exe
