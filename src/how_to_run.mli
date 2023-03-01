open! Core
open Async

(** Describes everything needed for Rpc_parallel to launch a worker. *)
type t

(** A local worker needs no special information. Rpc_parallel runs the same executable
    that the master is using. *)
val local : t

(** In order to run a worker remotely, you must ensure that the executable is available on
    the remote host. If [~assert_binary_hash:false] is provided, the check that the remote
    binary's hash matches the currently running binary's hash is skipped. *)
val remote : ?assert_binary_hash:bool -> _ Remote_executable.t -> t

(** [wrap] allows you to customize how the executable is launched. For example, you can
    run the worker via some wrapper command. *)
val wrap : t -> f:(Prog_and_args.t -> Prog_and_args.t) -> t

val run
  :  t
  -> env:(string * string) list
  -> worker_command_args:string list
  -> Process.t Or_error.t Deferred.t
