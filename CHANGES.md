## 113.24.00

- Switched to PPX.

- Expose the `connection_timeout` argument in rpc\_parallel. This argument
  exists in `Rpc_parallel_core.Parallel`, but it is not exposed in
  `Rpc_parallel.Parallel`.

- Allow custom handling of missed async\_rpc heartbeats.

- Give a better error message when redirecting output on a remote box to a file
  path that does not exist.

- remove unncessary chmod 700 call on the remote executable

- Give a clear error message for the common mistake of not making the
  `Parallel.Make_worker()` functor application top-level

- Make errors/exceptions in `Rpc_parallel` more observable

  - Make stderr and stdout redirection mandatory in order to encourage logging stderr
  - Clean up the use of monitors across `Rpc_parallel`
  - Fix bug with exceptions that are sent directly to `Monitor.main`
    (e.g. `Async_log` does this)

- Add the ability to explicitly initialize as a master and use some subcommand for the
  worker. This would allow writing programs with complex command structures that don't have
  to invoke a bunch of `Rpc_parallel` logic and start RPC servers for every command.


- Add the ability to get log messages from a worker sent back to the master.
  In fact, any worker can register for the log messages of any other workers.

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

