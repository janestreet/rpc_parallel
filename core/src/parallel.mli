open Core.Std
open Async.Std

module Fd_redirection : sig
  type t = [
    | `Dev_null
    | `File_append of string
    | `File_truncate of string
  ] with sexp
end

(*
   Implementation overview:

   Upon [run] (which should be top-level), the master process starts an Rpc server.
   When [spawn_worker] is called, an exact copy of the same executable will be run in
   another process (either locally or remotely). The [Remote_executable] module can be
   used to facilitate this transfer. This executable is then run through ssh. Various
   environment variables are set to communicate the worker's unique id and master host
   and port. The master waits to receieve a [register_worker_rpc]. Upon registration,
   the master sends a [worker_run_rpc].

   When [run] is called in a worker, the worker starts an Rpc server with an
   implementation for [worker_run_rpc]. This implementation calls [M.worker_main arg].
   The worker then sends a [register_worker_rpc] to the master with this
   [Host_and_port.t] of its own Rpc server.

   * Cleanup *
   - Worker processes must die when the master dies (unless [disown] is set to true).
   In order to achieve this, each worker calls [Shutdown.shutdown] upon
   [Rpc.Connection.is_closed connection] where [connection] is the initial connection
   established between the worker and the master, kept open to facilitate this cleanup.
   In this way, a master dieing will have the side effect of closing the Rpc connection,
   which will cause the workers to shutdown. In addition, a user can call [kill_worker] to
   cleanup a worker process manually.

   * Exception handling in the workers *
   - When creating a worker, the master should get notified of any exception that gets
   raised in any of the workers. Workers use [handle_worker_exn_rpc] to send an exception
   back to the master. In order to catch its exceptions, we schedule the call to
   [M.worker_main] within a new monitor and then detach this monitor and iterate over
   any exceptions that it catches. These exceptions are sent back to the master and
   reported through the [on_failure] callback.
*)

(* Basic usage:

   type A
   type B

   let worker_main : (A -> B Deferred.t) = ...

   module Parallel_app = Parallel.Make(struct
     type worker_arg = A with bin_io
     type worker_ret = B with bin_io
     let worker_main = worker_main
   end)

   let command = Command.async ~summary spec func

   let () = Parallel_app.run command
*)

(* This module is used to transfer the currently running executable to a remote machine *)
module Remote_executable : sig
  type 'a t

  (* [existing_on_host ~executable_path ?strict_host_key_checking host] will create a
     [t] from the supplied host and path. The executable MUST be the exact same executable
     that will be run in the master process. There will be a check for this in
     [spawn_worker]. Use [strict_host_key_checking] to change the StrictHostKeyChecking
     option used when sshing into this host *)
  val existing_on_host
    :  executable_path:string
    -> ?strict_host_key_checking:[`No | `Ask | `Yes]
    -> string
    -> [`Undeletable] t

  (* [copy_to_host ~executable_dir ?strict_host_key_checking host] will copy the currently
     running executable to the desired host and path. It will keep the same name but add a
     suffix .XXXXXXXX. Use [strict_host_key_checking] to change the StrictHostKeyChecking
     option used when sshing into this host *)
  val copy_to_host
    :  executable_dir:string
    -> ?strict_host_key_checking:[`No | `Ask | `Yes]
    -> string
    -> [`Deletable] t Or_error.t Deferred.t

  (* [delete t] will delete a remote executable that was copied over by a previous call
     to [copy_to_host] *)
  val delete : [`Deletable] t
    -> unit Or_error.t Deferred.t

  (* Get the underlying path and host *)
  val path : _ t -> string
  val host : _ t -> string
end

module Make(
  M : sig
    type worker_arg with bin_io
    type worker_ret with bin_io
    val worker_main : worker_arg -> worker_ret Deferred.t
  end) : sig

  type worker_id

  (* [spawn_worker arg ?where ?on_failure ?disown ()] will create a worker on [where] that
     runs [worker_main arg].

     [where] defaults to [`Local] but can be specified to be some remote host.

     Specifying [?log_dir] as a folder on the worker machine leads to the worker
     redirecting its stdout and stderr to files in this folder, named with the worker id.
     When [?name] is also specified, the log file will use this name instead.

     [on_failure] will be called when the spawned worker either loses connection
     or raises an exception. [on_failure] is run inside of [Monitor.main] so that any
     exceptions raised inside of the function will be seen top-level. Otherwise, these
     exceptions would be lost due to the internals of [Async.Rpc] and
     [Monitor.try_with f]'s default argument to ignore exceptions after [f ()] has been
     determined.

     [disown] defaults to false. If [disown] is set to [true] then this worker will not
     shutdown upon losing connection with the master process. Other than what happens when
     it loses its connection to the master, it behaves normally, including reporting
     errors to the master until it actually gets disconnected. The spawned workers will be
     running with some name [name.exe.XXXXXXXX] where [name.exe] was the name of the
     original running executable.

     WARNING: If you use the [disown] flag, be very careful; it makes it much harder to
     bring your program down cleanly, and you could easily flood your worker machines with
     orphaned workers. Make sure you actually need it before using it, and make sure to
     program defensively.)  *)

  val spawn_worker
    :  ?where : [`Local | `Remote of _ Remote_executable.t]
    -> ?disown : bool
    -> ?env : (string * string) list
    -> ?connection_timeout : Time.Span.t
    -> ?redirect_stdout : Fd_redirection.t  (** default redirect to /dev/null *)
    -> ?redirect_stderr : Fd_redirection.t  (** default redirect to /dev/null *)
    -> ?cd              : string            (** default / *)
    -> ?umask           : int               (** defaults to use existing umask *)
    -> M.worker_arg
    -> on_failure : (Error.t -> unit)
    -> (M.worker_ret * worker_id) Or_error.t Deferred.t

  val spawn_worker_exn
    :  ?where : [`Local | `Remote of _ Remote_executable.t]
    -> ?disown : bool
    -> ?env : (string * string) list
    -> ?connection_timeout : Time.Span.t
    -> ?redirect_stdout : Fd_redirection.t  (** default redirect to /dev/null *)
    -> ?redirect_stderr : Fd_redirection.t  (** default redirect to /dev/null *)
    -> ?cd              : string            (** default / *)
    -> ?umask           : int               (** defaults to use existing umask *)
    -> M.worker_arg
    -> on_failure : (Error.t -> unit)
    -> (M.worker_ret * worker_id) Deferred.t

  (* [kill_worker id] will close the established connection with the spawned worker and
     kill the worker process *)
  val kill_worker : worker_id -> unit Deferred.t

  (* [run command] should be called from the top-level in order to start the parallel
     application. [command] must be constructed with a call to [Command.async] so that
     the async scheduler is started *)
  val run : Command.t -> unit

end

