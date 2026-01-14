open Core
open Async

(** This module is primarily meant for backwards compatibility with code that used earlier
    versions of [Rpc_parallel]. Please consider using the [Rpc_parallel.Make()] functor
    instead as the semantics are more transparent and intuitive.

    This functor keeps cached connections to workers and dispatches on these connections.
    The semantics of reconnect are not currently well-defined. If you expect connections
    to drop or want multiple connections to the same worker, using [Rpc_parallel.Make()]
    is probably the better choice. *)

module type Worker = sig
  type t [@@deriving sexp_of]
  type unmanaged_t
  type 'a functions

  (** Accessor for the functions implemented by this worker type *)
  val functions : unmanaged_t functions

  type worker_state_init_arg
  type connection_state_init_arg

  module Id : Identifiable

  val id : t -> Id.t

  val spawn
    :  ?how:How_to_run.t
    -> ?name:string
    -> ?env:(string * string) list
    -> ?connection_timeout:Time_float.Span.t
    -> ?cd:string (** default / *)
    -> ?umask:int (** defaults to use existing umask *)
    -> redirect_stdout:Fd_redirection.t
    -> redirect_stderr:Fd_redirection.t
    -> worker_state_init_arg
    -> connection_state_init_arg
    -> on_failure:(Error.t -> unit) (** See [on_failure] in parallel_intf.ml *)
    -> on_connection_to_worker_closed:(Error.t -> unit)
         (** Called when the connection to the spawned worker is closed. If a worker
             process terminates, both [on_failure] and [on_connection_to_worker_closed]
             might get called. *)
    -> t Or_error.t Deferred.t

  val spawn_exn
    :  ?how:How_to_run.t
    -> ?name:string
    -> ?env:(string * string) list
    -> ?connection_timeout:Time_float.Span.t
    -> ?cd:string
    -> ?umask:int
    -> redirect_stdout:Fd_redirection.t
    -> redirect_stderr:Fd_redirection.t
    -> worker_state_init_arg
    -> connection_state_init_arg
    -> on_failure:(Error.t -> unit)
    -> on_connection_to_worker_closed:(Error.t -> unit)
    -> t Deferred.t

  (** [run t] and [run_exn t] will connect to [t] if there is not already a connection,
      but if there is currently a connection that has gone stale, they will fail with an
      error. Trying again will attempt a reconnection. *)
  val run
    :  ?on_pipe_rpc_close_error:(Error.t -> unit)
    -> t
    -> f:(unmanaged_t, 'query, 'response) Parallel.Function.t
    -> arg:'query
    -> 'response Or_error.t Deferred.t

  val run_exn
    :  ?on_pipe_rpc_close_error:(Error.t -> unit)
    -> t
    -> f:(unmanaged_t, 'query, 'response) Parallel.Function.t
    -> arg:'query
    -> 'response Deferred.t

  (** Using these functions will not result in [on_failure] reporting a closed connection,
      unlike running the [shutdown] function. *)
  val kill : t -> unit Or_error.t Deferred.t

  val kill_exn : t -> unit Deferred.t
end

module Make (S : Parallel.Worker_spec) :
  Worker
  with type 'a functions := 'a S.functions
   and type worker_state_init_arg := S.Worker_state.init_arg
   and type connection_state_init_arg := S.Connection_state.init_arg
