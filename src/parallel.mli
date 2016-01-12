open Core.Std
open Async.Std

(** A type-safe parallel library built on top of Async_rpc.

    module Worker = Parallel.Make_worker(T:Worker_spec)

    The [Worker] module can be used to spawn new workers, either locally or remotely, and
    run functions on these workers. [T] specifies which functions can be run on a
    [Worker.t] as well as the implementations for these functions. In addition, different
    instances of [Worker.t] types can be differentiated upon [spawn] by passing in a
    different [init_arg] and handling this argument in the [init] function of [T].

    Basic usage:

    module Worker = struct
      module T = struct
        type 'worker functions = {f1:('worker, int, int) Parallel.Function.t}

        type init_arg = unit with bin_io
        let init = return

        module Functions(C:Parallel.Creator) = struct
          let f1 = C.create_rpc ~f:(fun i -> return (i+42)) ~bin_input:Int.bin_t
            ~bin_output:Int.bin_t

          let functions = {f1}
        end
      end
      include Parallel.Make_worker(T)
    end

    let main ... =
      ...
      Worker.spawn_exn ~on_failure:Error.raise () >>= fun worker ->
      ...
      Worker.run_exn worker ~f:Worker.functions.f1 ~arg:0 >>= fun res ->
      ...

    let command = Command.async ~summary spec main

    let () = Parallel.start_app command

    * NOTE *: It is highly recommended for [Parallel.start_app] and [Parallel.Make_worker]
    calls to be top-level. But the only real requirements are:

    1) The master's state is initialized before any calls to [spawn]. This will be
    achieved either by [Parallel.start_app] or [Parallel.init_master_exn].

    2) Spawned workers (runs of your executable with a certain environment variable set)
    must start running as a worker. This will be achieved either by [Parallel.start_app]
    or [Parallel.run_as_worker_exn]. With [Parallel.run_as_worker_exn], you must carefully
    supply the necessary command arguments to ensure [Parallel.run_as_worker_exn] is
    indeed called.

    3) Spawned workers must be able to find their function implementations when they start
    running as a worker. These implementations are gathered on the application of the
    [Parallel.Make_worker] functor.

    A word on monitoring:

    Exceptions in workers will always result in the worker shutting down. The master can
    be notified of these exceptions in multiple ways:

    - If the exception occured in a function implementation before return, the exception
      will be returned back in the [Error.t] of the [run] (or [spawn]) call.

    - If the exception occured after return, [on_failure exn] will be called.

    - If [redirect_stderr] specifies a file, the worker will write its exception to that
      file before shutting down. *)

(** This module is used to transfer the currently running executable to a remote machine *)
module Remote_executable : sig
  type 'a t

  (** [existing_on_host ~executable_path ?strict_host_key_checking host] will create a
     [t] from the supplied host and path. The executable MUST be the exact same executable
     that will be run in the master process. There will be a check for this in
     [spawn_worker]. Use [strict_host_key_checking] to change the StrictHostKeyChecking
     option used when sshing into this host *)
  val existing_on_host
    :  executable_path:string
    -> ?strict_host_key_checking:[`No | `Ask | `Yes]
    -> string
    -> [`Undeletable] t

  (** [copy_to_host ~executable_dir ?strict_host_key_checking host] will copy the currently
     running executable to the desired host and path. It will keep the same name but add a
     suffix .XXXXXXXX. Use [strict_host_key_checking] to change the StrictHostKeyChecking
     option used when sshing into this host *)
  val copy_to_host
    :  executable_dir:string
    -> ?strict_host_key_checking:[`No | `Ask | `Yes]
    -> string
    -> [`Deletable] t Or_error.t Deferred.t

  (** [delete t] will delete a remote executable that was copied over by a previous call
     to [copy_to_host] *)
  val delete : [`Deletable] t -> unit Or_error.t Deferred.t

  (** Get the underlying path, host, and host_key_checking *)
  val path : _ t -> string
  val host : _ t -> string
  val host_key_checking : _ t -> string list
end

module Fd_redirection : sig
  type t = [
    | `Dev_null
    | `File_append of string
    | `File_truncate of string
  ] [@@deriving sexp]
end

(** A [('worker, 'query, 'response) Function.t] is a type-safe function ['query ->
    'response Deferred.t] that can only be run on a ['worker]. Under the hood it
    represents an Async Rpc protocol that we know a ['worker] will implement. *)
module Function : sig
  type ('worker, 'query, 'response) t
end

module type Creator = sig
  type worker [@@deriving bin_io]
  type state

  (** [create_rpc ?name ~f ~bin_input ~bin_output ()] will create an [Rpc.Rpc.t] with
      [name] if specified and use [f] as an implementation for this Rpc. It returns back a
      [Function.t], a type-safe [Rpc.Rpc.t]. *)
  val create_rpc
    :  ?name: string
    -> f:(state -> 'query -> 'response Deferred.t)
    -> bin_input: 'query Bin_prot.Type_class.t
    -> bin_output: 'response Bin_prot.Type_class.t
    -> unit
    -> (worker, 'query, 'response) Function.t

  (** [create_pipe ?name ~f ~bin_input ~bin_output ()] will create an [Rpc.Pipe_rpc.t]
      with [name] if specified. The implementation for this Rpc is a function that creates
      a [Pipe.Reader.t] and a [Pipe.Writer.t], then calls [f arg ~writer] and returns the
      reader. [create_pipe] returns a [Function_piped.t] which is a type-safe
      [Rpc.Pipe_rpc.t]. *)
  val create_pipe :
    ?name: string
    -> f:(state -> 'query -> 'response Pipe.Reader.t Deferred.t)
    -> bin_input: 'query Bin_prot.Type_class.t
    -> bin_output: 'response Bin_prot.Type_class.t
    -> unit
    -> (worker, 'query, 'response Pipe.Reader.t) Function.t

  (** [of_async_rpc ~f rpc] is the analog to [create_rpc] but instead of creating an Rpc
      protocol, it uses the supplied one *)
  val of_async_rpc
    :  f:(state -> 'query -> 'response Deferred.t)
    -> ('query, 'response) Rpc.Rpc.t
    -> (worker, 'query, 'response) Function.t

  (** [of_async_pipe_rpc ~f rpc] is the analog to [create_pipe] but instead of creating a
      Pipe_rpc protocol, it uses the supplied one *)
  val of_async_pipe_rpc
    :  f:(state -> 'query -> 'response Pipe.Reader.t Deferred.t)
    -> ('query, 'response, Error.t) Rpc.Pipe_rpc.t
    -> (worker, 'query, 'response Pipe.Reader.t) Function.t

  (** [run] is exposed in this interface so the implementations of functions can include
      running other functions defined on this worker. A function can take a [worker] as an
      argument and call [run] on this worker *)
  val run
    :  worker
    -> f: (worker, 'query, 'response) Function.t
    -> arg: 'query
    -> 'response Or_error.t Deferred.t

  val run_exn
    :  worker
    -> f: (worker, 'query, 'response) Function.t
    -> arg: 'query
    -> 'response Deferred.t
end

module type Functions = sig
  type worker
  type 'worker functions
  val functions : worker functions
end

module type Worker = sig
  (** A [Worker.t] type is defined [with bin_io] so it is possible to create functions
      that take a worker as an argument. See sample code for examples *)
  type t [@@deriving bin_io]
  type 'a functions
  type init_arg

  (** Accessor for the functions implemented by this worker type *)
  val functions : t functions

  (** [spawn_worker arg ?where ?on_failure ?disown ()] will create a worker on [where]
      that can subsequently run some functions.

      [where] defaults to [`Local] but can be specified to be some remote host.

      Specifying [?log_dir] as a folder on the worker machine leads to the worker
      redirecting its stdout and stderr to files in this folder, named with the worker id.
      When [?name] is also specified, the log file will use this name instead.

      [on_failure] will be called when the spawned worker either loses connection
      or raises a background exception.

      [disown] defaults to false. If it is set to [true] then this worker will not
      shutdown upon losing connection with the master process. It will continue to report
      back any exceptions as long as it remains connected to the master. The spawned
      workers will be running with some name [name.exe.XXXXXXXX] where [name.exe] was the
      name of the original running executable. *)
  val spawn
    :  ?where : [`Local | `Remote of _ Remote_executable.t]
    -> ?disown : bool
    -> ?env : (string * string) list
    -> ?rpc_max_message_size  : int
    -> ?rpc_handshake_timeout : Time.Span.t
    -> ?rpc_heartbeat_config : Rpc.Connection.Heartbeat_config.t
    -> ?connection_timeout:Time.Span.t
    -> ?cd : string  (** default / *)
    -> ?umask : int  (** defaults to use existing umask *)
    -> redirect_stdout : Fd_redirection.t
    -> redirect_stderr : Fd_redirection.t
    -> init_arg
    -> on_failure : (Error.t -> unit)
    -> t Or_error.t Deferred.t

  val spawn_exn
    :  ?where : [`Local | `Remote of _ Remote_executable.t]
    -> ?disown : bool
    -> ?env : (string * string) list
    -> ?rpc_max_message_size  : int
    -> ?rpc_handshake_timeout : Time.Span.t
    -> ?rpc_heartbeat_config : Rpc.Connection.Heartbeat_config.t
    -> ?connection_timeout:Time.Span.t
    -> ?cd : string  (** default / *)
    -> ?umask : int  (** defaults to use existing umask *)
    -> redirect_stdout : Fd_redirection.t
    -> redirect_stderr : Fd_redirection.t
    -> init_arg
    -> on_failure : (Error.t -> unit)
    -> t Deferred.t

  (** [run t ~f ~arg] will run [f] on [t] with the argument [arg]. *)
  val run
    :  t
    -> f: (t, 'query, 'response) Function.t
    -> arg: 'query
    -> 'response Or_error.t Deferred.t

  val run_exn
    :  t
    -> f: (t, 'query, 'response) Function.t
    -> arg: 'query
    -> 'response Deferred.t

  (** [async_log t] will return a [Pipe.Reader.t] collecting messages corresponding to
      [Log.Global] calls in [t]. You need not be the master of [t] to get its log.

      Note: there is no queueing of log messages on the worker side, so all log messages
      that were written before the call to [async_log] will not be written to the
      pipe. A consequence of this is that you will never get any log messages written in
      a worker's init function. *)
  val async_log : t -> Log.Message.Stable.V2.t Pipe.Reader.t Deferred.Or_error.t


  (** [kill t] will close the established connection with the spawned worker and kill the
      worker process. Subsequent calls to [run] or [kill] on this worker will result in an
      error. [kill] only works from the master that initially spawned the worker, and will
      fail with an error if you run it from any other process. *)
  val kill     : t -> unit Or_error.t Deferred.t
  val kill_exn : t -> unit            Deferred.t

  (** Get the underlying host/port information of the given worker *)
  val host_and_port : t -> Host_and_port.t
end

(** Specification for the creation of a worker type *)
module type Worker_spec = sig
  (** A type to encapsulate all the functions that can be run on this worker. Using a
      record type here is often the most convenient and readable *)
  type 'worker functions

  (** [init init_arg] will be called in a worker when it is spawned *)
  type init_arg [@@deriving bin_io]
  type state
  val init : init_arg -> state Deferred.t

  (** The functions that can be run on this worker type *)
  module Functions(C:Creator with type state := state) : Functions
    with type 'a functions := 'a functions
    with type worker := C.worker
end

(** module Worker = Make_worker(T)

    The [Worker] module has specialized functions to spawn workers, kill workers, and run
    functions on workers. *)
module Make_worker(S: Worker_spec) : Worker
  with type 'a functions := 'a S.functions
  with type init_arg := S.init_arg

(** [start_app command] should be called from the top-level in order to start the parallel
    application. [command] must be constructed with a call to [Command.async] so that the
    Async scheduler is started. This function will parse certain environment variables and
    determine whether to start as a master or a worker.

    [rpc_max_message_size], [rpc_heartbeat_config], [where_to_listen] specify the RPC
    server that the master starts.

    [implementations] are additional implementations that the master will implement.
*)
val start_app
  : ?rpc_max_message_size : int
  -> ?rpc_handshake_timeout : Time.Span.t
  -> ?rpc_heartbeat_config : Rpc.Connection.Heartbeat_config.t
  (** You should almost always let the OS choose what host/port the master's rpc server
      lives on. *)
  -> ?where_to_listen : Tcp.Where_to_listen.inet
  -> ?implementations : unit Rpc.Implementation.t list
  -> Command.t
  -> unit

(** Use [State.get] to query whether [start_app] was used. We return a [State.t] rather
    than a [bool] so that you can require evidence at the type level. If you want to
    certify, as a precondition, for some function that [start_app] was used, require a
    [State.t] as an argument. If you don't need the [State.t] anymore, just pattern match
    on it. *)
module State : sig
  type t = private [< `started ]

  val get : unit -> t option
end

(** If you want more direct control over your executable, you can use the [Expert]
    module instead of [start_app]. If you use [Expert], you are responsible for
    initializing the master service or running the worker. [worker_command_args] will be
    the arguments sent to each spawned worker. Running your executable with these args
    must follow a code path that calls [run_as_worker_exn]. *)
module Expert : sig
  (** [init_master_exn] should be called in the single master process. It is necessary to
      be able to spawn workers. *)
  val init_master_exn
    :  ?rpc_max_message_size  : int
    -> ?rpc_handshake_timeout : Time.Span.t
    -> ?rpc_heartbeat_config : Rpc.Connection.Heartbeat_config.t
    -> ?where_to_listen : Tcp.Where_to_listen.inet
    -> ?implementations : unit Rpc.Implementation.t list
    -> worker_command_args : string list
    -> unit
    -> unit

  (** [run_as_worker_exn] is the entry point for all workers. It is illegal to call both
      [init_master_exn] and [run_as_worker_exn] in the same process. Will also raise an
      exception if the process was not spawned by a master. Make sure to pass in
      [worker_command_args] here as well so this spawned worker can also successfully
      spawn workers.

      NOTE: Various information is sent from the master to the spawned worker as a sexp
      through its stdin. A spawned worker should *never* read from stdin before calling
      [run_as_worker_exn].
  *)
  val run_as_worker_exn
    :  worker_command_args : string list
    -> never_returns
end
