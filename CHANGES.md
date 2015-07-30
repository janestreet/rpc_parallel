## 113.00.00

- Fixed a file-descriptor leak

    There was a file descriptor leak when killing workers.  Their stdin,
    stdout, and stderr remain open. We now close them after the worker process
    exits.

## 112.24.00

- Added `Parallel.State.get` function, to check whether `Rpc_parallel` has been
  initialized correctly.
- Added `Map_reduce` module, which is an easy-to-use parallel map/reduce library.

  It can be used to map/fold over a list while utilizing multiple cores on multiple machines.

  Also added support for workers to keep their own inner state.

- Fixed bug in which  zombie process was created per spawned worker.

  Also fixed shutdown on remote workers

- Made it possible for workers to spawn other workers, i.e.  act as masters.

- Made the connection timeout configurable and bumped the default to 10s.

## 112.17.00

- Follow changes in Async RPC

## 112.01.00

Initial import.

