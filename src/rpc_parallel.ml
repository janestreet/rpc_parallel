open! Core

(** A type-safe parallel library built on top of Async_rpc.

    {[
      module Worker = Rpc_parallel.Make ((T : Worker_spec))
    ]}

    The [Worker] module can be used to spawn new workers, either locally or remotely, and
    run functions on these workers. [T] specifies which functions can be run on a
    [Worker.t] as well as the implementations for these functions. In addition, [T]
    specifies worker states and connection states. See README for more details *)

module Fd_redirection = Fd_redirection
module How_to_run = How_to_run
module Map_reduce = Map_reduce
module Prog_and_args = Prog_and_args
module Remote_executable = Remote_executable
module Rpc_settings = Rpc_settings
module Utils = Utils

module Managed = Managed
[@@alert legacy "Prefer using the plain [Rpc_parallel] instead of [Rpc_parallel.Managed]"]

include Parallel

module Parallel = Parallel
[@@deprecated "[since 2016-11] Use [Rpc_parallel] instead of [Rpc_parallel.Parallel]"]
