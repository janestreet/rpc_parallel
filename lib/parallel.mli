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

    * NOTE *: The calls to [Parallel.start_app] and [Parallel.Make_worker] calls MUST
    be top-level. *)

(** A [('worker, 'query, 'response) Function.t] is a type-safe function ['query ->
    'response Deferred.t] that can only be run on a ['worker]. Under the hood it
    represents some sort of Async Rpc protocol. *)
module Function : sig
  type ('worker, 'query, 'response) t
end

module Fd_redirection = Rpc_parallel_core.Std.Parallel.Fd_redirection

module type Creator = sig
  type worker with bin_io
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

(** This module is used to transfer the currently running executable to a remote
    machine *)
module Remote_executable : module type of Rpc_parallel_core.Std.Parallel.Remote_executable
  with type 'a t = 'a Rpc_parallel_core.Std.Parallel.Remote_executable.t

module type Worker = sig
  (** A [Worker.t] type is defined [with bin_io] so it is possible to create functions
      that take a worker as an argument. See sample code for examples *)
  type t with bin_io
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
      or raises an exception. [on_failure] is run inside of [Monitor.main] so that any
      exceptions raised inside of the function will be seen top-level. Otherwise, these
      exceptions would be lost due to the internals of [Async.Rpc] and
      [Monitor.try_with f]'s default argument to ignore exceptions after [f ()] has been
      determined.

      [disown] defaults to false. If it is set to [true] then this worker will not
      shutdown upon losing connection with the master process. It will continue to report
      back any exceptions as long as it remains connected to the master. The spawned
      workers will be running with some name [name.exe.XXXXXXXX] where [name.exe] was the
      name of the original running executable. *)
  val spawn
    :  ?where : [`Local | `Remote of _ Remote_executable.t]
    -> ?disown : bool
    -> ?env : (string * string) list
    -> ?redirect_stdout : Fd_redirection.t  (** default redirect to /dev/null *)
    -> ?redirect_stderr : Fd_redirection.t  (** default redirect to /dev/null *)
    -> ?cd : string  (** default / *)
    -> ?umask : int  (** defaults to use existing umask *)
    -> init_arg
    -> on_failure : (Error.t -> unit)
    -> t Or_error.t Deferred.t

  val spawn_exn
    :  ?where : [`Local | `Remote of _ Remote_executable.t]
    -> ?disown : bool
    -> ?env : (string * string) list
    -> ?redirect_stdout : Fd_redirection.t  (** default redirect to /dev/null *)
    -> ?redirect_stderr : Fd_redirection.t  (** default redirect to /dev/null *)
    -> ?cd : string  (** default / *)
    -> ?umask : int  (** defaults to use existing umask *)
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


  (** [disconnect t] will close the established connection with the spawned worker. The
      worker process will still remain alive and will automatically be reconnected with
      upon running another function. *)
  val disconnect : t -> unit Deferred.t

  (** [kill t] will close the established connection with the spawned worker and kill the
      worker process. Subsequent calls to [run] on this worker type will result in an
      error *)
  val kill : t -> unit Deferred.t

  (** Get the underlying host/port information of the given worker *)
  val host_and_port : t -> Host_and_port.t

end

(** Specification for the creation of a worker type *)
module type Worker_spec = sig
  (** A type to encapsulate all the functions that can be run on this worker. Using a
      record type here is often the most convenient and readable *)
  type 'worker functions

  (** [init init_arg] will be called in a worker when it is spawned *)
  type init_arg with bin_io
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

(** [run command] should be called from the top-level in order to start the parallel
    application. [command] must be constructed with a call to [Command.async] so that the
    async scheduler is started *)
val start_app : Command.t -> unit

(** Use [State.get] to query whether [start_app] was used. We return a [State.t] rather
    than a [bool] so that you can require evidence at the type level. If you want to
    certify, as a precondition, for some function that [start_app] was used, require a
    [State.t] as an argument. If you don't need the [State.t] anymore, just pattern match
    on it. *)
module State : sig
  type t = private [< `started ]

  val get : unit -> t option
end
