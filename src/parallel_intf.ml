open! Core
open! Async

(** See the doc/ directory for more details *)

(** A [('worker, 'query, 'response) Function.t] is a type-safe function ['query ->
    'response Deferred.t] that can only be run on a ['worker]. Under the hood it
    represents an Async Rpc protocol that we know a ['worker] will implement. *)
module type Function = sig
  type ('worker, 'query, +'response) t

  module Direct_pipe : sig
    module Id : sig
      type 'worker t
    end

    type nonrec ('worker, 'query, 'response) t =
      ( 'worker
      , 'query * ('response Rpc.Pipe_rpc.Pipe_message.t -> Rpc.Pipe_rpc.Pipe_response.t)
      , 'worker Id.t )
      t
  end

  val map : ('worker, 'query, 'a) t -> f:('a -> 'b) -> ('worker, 'query, 'b) t

  val contra_map
    :  ('worker, 'a, 'response) t
    -> f:('b -> 'a)
    -> ('worker, 'b, 'response) t

  (** Common functions that are implemented by all workers *)

  (** This implementation will add another [Log.Output] for [Log.Global] that transfers
      log messages to the returned pipe. You can subscribe to a worker's log more than
      once and from different processes, as each call simply adds a new [Log.Output].
      Closing the pipe will remove the corresponding [Log.Output].

      NOTE: You will never get any log messages before this implementation has run (there
      is no queuing of log messages). As a consequence, you will never get any log
      messages written in a worker's init functions. *)
  val async_log : (_, unit, Log.Message.Stable.V2.t Pipe.Reader.t) t

  (** A given process can have multiple worker servers running (of the same or different
      worker types). This implementation closes the server on which it is run. All
      existing open connections will remain open, but no further connections to this
      worker server will be accepted.

      NOTE: calling [close_server] on a worker process that is only running one worker
      server will leave a stranded worker process if no other cleanup has been setup
      (e.g. setting up [on_client_disconnect] or [Connection.close_finished] handlers) *)
  val close_server : (_, unit, unit) t

  module For_internal_testing : sig
    val worker_server_rpc_settings : (_, unit, Rpc_settings.t) t
  end
end

module type Worker = sig
  type ('worker, 'query, 'response) _function
  type 'worker _id_direct

  (** A [Worker.t] type is defined [with bin_io] so it is possible to create functions
      that take a worker as an argument. *)
  type t [@@deriving bin_io, sexp_of]

  (** A type alias to make the [Connection] signature more readable *)
  type worker = t

  type 'a functions

  (** Accessor for the functions implemented by this worker type *)
  val functions : t functions

  type worker_state_init_arg
  type connection_state_init_arg

  module Id : Identifiable

  val id : t -> Id.t
  val rpc_settings : t -> Rpc_settings.t

  (** [serve arg] will start an Rpc server in process implementing all the functions
      of the given worker. *)
  val serve : worker_state_init_arg -> worker Deferred.t

  module Connection : sig
    type t [@@deriving sexp_of]

    (** The [id] of the connected worker *)
    val worker_id : t -> Id.t

    (** Run functions implemented by this worker *)
    val run
      :  t
      -> f:(worker, 'query, 'response) _function
      -> arg:'query
      -> 'response Or_error.t Deferred.t

    val run_exn
      :  t
      -> f:(worker, 'query, 'response) _function
      -> arg:'query
      -> 'response Deferred.t

    (** Can be used to abort a direct pipe function in progress. *)
    val abort : t -> id:worker _id_direct -> unit

    (** Connect to a given worker, returning a type wrapped [Rpc.Connection.t] that can be
        used to run functions. *)
    val client : worker -> connection_state_init_arg -> t Or_error.t Deferred.t

    val client_exn : worker -> connection_state_init_arg -> t Deferred.t

    (** [with_client worker init_arg f] connects to the [worker]'s server, initializes the
        connection state with [init_arg]  and runs [f] until an exception is thrown or
        until the returned Deferred is determined.

        NOTE: You should be careful when using this with [Pipe_rpc].
        See [Rpc.Connection.with_close] for more information. *)
    val with_client
      :  worker
      -> connection_state_init_arg
      -> f:(t -> 'a Deferred.t)
      -> 'a Or_error.t Deferred.t

    val close : t -> unit Deferred.t
    val close_finished : t -> unit Deferred.t
    val close_reason : t -> on_close:[ `started | `finished ] -> Info.t Deferred.t
    val is_closed : t -> bool
  end

  module Shutdown_on (M : T1) : sig
    (** This variant determines when a spawned worker will shut itself down.

        In both the [Connection_closed] and [Heartbeater_connection_timeout] cases, the
        worker will shut itself down when [Rpc.Connection.close_finished] is determined on
        some established connection. The difference is which connection.

        In the [Connection_closed] case, the connection is returned back to the caller.
        The caller can call [Worker.Connection.close] to cause worker shutdown. If you
        never intend to reconnect to your spawned worker, this is probably the variant you
        want to choose.

        In the [Heartbeater_connection_timeout] case, the connection is internal to the
        library. The [worker] is returned so the caller is free to establish
        connections and close them without triggering worker shutdown.

        In both the above cases, worker shutdown will be triggered when the master process
        exits. It may also result from network problems or long async cycles.

        In the [Called_shutdown_function] case, the worker will only shut itself down on
        an explicit [Worker.shutdown] call. Worker's spawned with this variant do not
        shutdown when the master process exits. We strongly discourage use of this
        variant. *)
    type _ t =
      | Connection_closed
          : (connection_state_init_arg:connection_state_init_arg
             -> Connection.t M.t Deferred.t)
            t
      | Heartbeater_connection_timeout : worker M.t Deferred.t t
      | Called_shutdown_function : worker M.t Deferred.t t
  end

  (** The various [spawn] functions create a new worker process that implements the
      functions specified in the [Worker_spec].

      [name] will be attached to certain error messages and is useful for debugging.

      [env] extends the environment of the spawned worker process.

      [connection_timeout] is used for various internal timeouts. This may need be to
      increased if the init arg is really large (serialization and deserialization
      takes more than [connection_timeout]).

      [cd] changes the current working directory of a spawned worker process.

      [shutdown_on] specifies when a worker should shut itself down.

      [on_failure exn] will be called in the spawning process upon the worker process
      raising a background exception. All exceptions raised before functions return will be
      returned to the caller. [on_failure] will be called in [Monitor.current ()] at the
      time of this spawn call. The worker initiates shutdown upon sending the exception
      to the master process.

      [worker_state_init_arg] (below) will be passed to [init_worker_state] of the given
      [Worker_spec] module. This initializes a persistent worker state for all connections
      to this worker. *)
  type 'a with_spawn_args =
    ?how:How_to_run.t (** default [How_to_run.local] *)
    -> ?name:string
    -> ?env:(string * string) list
    -> ?connection_timeout:Time_float.Span.t (** default 10 sec *)
    -> ?cd:string (** default / *)
    -> on_failure:(Error.t -> unit)
    -> 'a

  (** The spawned worker process daemonizes. Any initialization errors that wrote to
      stderr (Rpc_parallel internal initialization, not user initialization code) will be
      captured and rewritten to the spawning process's stderr with the prefix
      "[WORKER %NAME% STDERR]".

      [redirect_stdout] and [redirect_stderr] specify stdout and stderr of the worker
      process. *)
  val spawn
    : (?umask:int (** defaults to use existing umask *)
       -> shutdown_on:'a Shutdown_on(Or_error).t
       -> redirect_stdout:Fd_redirection.t
       -> redirect_stderr:Fd_redirection.t
       -> worker_state_init_arg
       -> 'a)
      with_spawn_args

  val spawn_exn
    : (?umask:int (** defaults to use existing umask *)
       -> shutdown_on:'a Shutdown_on(Monad.Ident).t
       -> redirect_stdout:Fd_redirection.t
       -> redirect_stderr:Fd_redirection.t
       -> worker_state_init_arg
       -> 'a)
      with_spawn_args

  module Spawn_in_foreground_result : sig
    type 'a t = ('a * Process.t) Or_error.t
  end

  (** Similar to [spawn] but the worker process does not daemonize. If the process was
      spawned on a remote host, the ssh [Process.t] is returned.

      Remember to call [Process.wait] on the returned [Process.t] to avoid a zombie
      process. Once the process exits, remember to call [Writer.close] on [Process.stdin]
      and [Reader.close] on [Process.stdout] and [Process.stderr] to close the process's
      stdin/stdout/stderr. *)
  val spawn_in_foreground
    : (shutdown_on:'a Shutdown_on(Spawn_in_foreground_result).t
       -> worker_state_init_arg
       -> 'a)
      with_spawn_args

  module Spawn_in_foreground_exn_result : sig
    type 'a t = 'a * Process.t
  end

  val spawn_in_foreground_exn
    : (shutdown_on:'a Shutdown_on(Spawn_in_foreground_exn_result).t
       -> worker_state_init_arg
       -> 'a)
      with_spawn_args

  (** [shutdown] attempts to connect to a worker. Upon success, [Shutdown.shutdown 0] is
      run in the worker. If you want strong guarantees that a worker did shutdown, consider
      using [spawn_in_foreground] and inspecting the [Process.t]. *)
  val shutdown : t -> unit Or_error.t Deferred.t

  module Deprecated : sig
    (** This is nearly identical to calling [spawn ~shutdown_on:Heartbeater_connection_timeout] and
        then [Connection.client]. The only difference is that this function handles
        shutting down the worker when [Connection.client] returns an error.

        Uses of [spawn_and_connect] that disregard [t] can likely be replaced with [spawn
        ~shutdown_on:Connection_closed]. If [t] is used for reconnecting, then you can use [spawn]
        followed by [Connection.client]. *)
    val spawn_and_connect
      : (?umask:int
         -> redirect_stdout:Fd_redirection.t
         -> redirect_stderr:Fd_redirection.t
         -> connection_state_init_arg:connection_state_init_arg
         -> worker_state_init_arg
         -> (t * Connection.t) Or_error.t Deferred.t)
        with_spawn_args

    val spawn_and_connect_exn
      : (?umask:int
         -> redirect_stdout:Fd_redirection.t
         -> redirect_stderr:Fd_redirection.t
         -> connection_state_init_arg:connection_state_init_arg
         -> worker_state_init_arg
         -> (t * Connection.t) Deferred.t)
        with_spawn_args
  end

  (** This module is used for internal testing of the rpc_parallel library. *)
  module For_internal_testing : sig
    module Spawn_in_foreground_result : sig
      type 'a t =
        ( 'a * Process.t
        , Error.t * [ `Worker_process of Unix.Exit_or_signal.t Deferred.t option ] )
        Result.t
    end

    val spawn_in_foreground
      : (shutdown_on:'a Shutdown_on(Spawn_in_foreground_result).t
         -> worker_state_init_arg
         -> 'a)
        with_spawn_args

    val master_app_rpc_settings : unit -> Rpc_settings.t
  end
end

module type Functions = sig
  type worker
  type worker_state_init_arg
  type worker_state
  type connection_state_init_arg
  type connection_state
  type 'worker functions

  val functions : worker functions

  (** [init_worker_state] is called with the [init_arg] passed to [spawn] or [serve] *)
  val init_worker_state : worker_state_init_arg -> worker_state Deferred.t

  (** [init_connection_state] is called with the [init_arg] passed to [Connection.client]

      [connection] should only be used to register [close_finished] callbacks, not to
      dispatch.  *)
  val init_connection_state
    :  connection:Rpc.Connection.t
    -> worker_state:worker_state
    -> connection_state_init_arg
    -> connection_state Deferred.t
end

module type Creator = sig
  type ('worker, 'query, 'response) _function
  type ('worker, 'query, 'response) _direct
  type worker
  type worker_state
  type connection_state

  (** [create_rpc ?name ~f ~bin_input ~bin_output ()] will create an [Rpc.Rpc.t] with
      [name] if specified and use [f] as an implementation for this Rpc. It returns back a
      [_function], a type-safe Rpc protocol. *)
  val create_rpc
    :  ?name:string
    -> f:
         (worker_state:worker_state
          -> conn_state:connection_state
          -> 'query
          -> 'response Deferred.t)
    -> bin_input:'query Bin_prot.Type_class.t
    -> bin_output:'response Bin_prot.Type_class.t
    -> unit
    -> (worker, 'query, 'response) _function

  (** [create_pipe ?name ~f ~bin_input ~bin_output ()] will create an [Rpc.Pipe_rpc.t]
      with [name] if specified. The implementation for this Rpc is a function that creates
      a [Pipe.Reader.t] and a [Pipe.Writer.t], then calls [f arg ~writer] and returns the
      reader.

      Notice that [aborted] is not exposed. The pipe is closed upon aborted. *)
  val create_pipe
    :  ?name:string
    -> ?client_pushes_back:unit
    -> f:
         (worker_state:worker_state
          -> conn_state:connection_state
          -> 'query
          -> 'response Pipe.Reader.t Deferred.t)
    -> bin_input:'query Bin_prot.Type_class.t
    -> bin_output:'response Bin_prot.Type_class.t
    -> unit
    -> (worker, 'query, 'response Pipe.Reader.t) _function

  (** [create_direct_pipe ?name ~f ~bin_input ~bin_output ()] will create an
      [Rpc.Pipe_rpc.t] with [name] if specified.

      As per the documentation at lib/async_rpc_kernel/src/rpc.mli:

      Though the implementation function is given a writer immediately, the result of the
      client's call to [dispatch] will not be determined until after the implementation
      function returns. Elements written before the function returns will be queued up to
      be written after the function returns. *)
  val create_direct_pipe
    :  ?name:string
    -> ?client_pushes_back:unit
    -> f:
         (worker_state:worker_state
          -> conn_state:connection_state
          -> 'query
          -> 'response Rpc.Pipe_rpc.Direct_stream_writer.t
          -> unit Deferred.t)
    -> bin_input:'query Bin_prot.Type_class.t
    -> bin_output:'response Bin_prot.Type_class.t
    -> unit
    -> (worker, 'query, 'response) _direct

  (** [create_one_way ?name ~f ~bin_msg ()] will create an [Rpc.One_way.t] with [name] if
      specified and use [f] as an implementation. *)
  val create_one_way
    :  ?name:string
    -> f:(worker_state:worker_state -> conn_state:connection_state -> 'query -> unit)
    -> bin_input:'query Bin_prot.Type_class.t
    -> unit
    -> (worker, 'query, unit) _function

  (** [create_reverse_pipe ?name ~f ~bin_query ~bin_update ~bin_response ()] generates a
      function allowing you to send a [query] and a pipe of [update]s to a worker. The
      worker will send back a [response]. It is up to you whether to send a [response]
      before or after finishing with the pipe; Rpc_parallel doesn't care. *)
  val create_reverse_pipe
    :  ?name:string
    -> ?client_pushes_back:unit
    -> f:
         (worker_state:worker_state
          -> conn_state:connection_state
          -> 'query
          -> 'update Pipe.Reader.t
          -> 'response Deferred.t)
    -> bin_query:'query Bin_prot.Type_class.t
    -> bin_update:'update Bin_prot.Type_class.t
    -> bin_response:'response Bin_prot.Type_class.t
    -> unit
    -> (worker, 'query * 'update Pipe.Reader.t, 'response) _function

  (** [create_reverse_direct_pipe ?name ~f ~bin_query ~bin_update ~bin_response ()]
      generates a function allowing you to send a [query] and a direct stream of [update]s
      to a worker. The worker will send back a [response]. It is up to you whether to send
      a [response] before or after finishing with the pipe; Rpc_parallel doesn't care. *)
  val create_reverse_direct_pipe
    :  ?name:string
    -> ?client_pushes_back:unit
    -> f:
         (worker_state:worker_state
          -> conn_state:connection_state
          -> 'query
          -> 'update Pipe.Reader.t
          -> 'response Deferred.t)
    -> bin_query:'query Bin_prot.Type_class.t
    -> bin_update:'update Bin_prot.Type_class.t
    -> bin_response:'response Bin_prot.Type_class.t
    -> unit
    -> ( worker
       , 'query
         * ('update Rpc.Pipe_rpc.Direct_stream_writer.t -> unit Or_error.t Deferred.t)
       , 'response )
       _function

  (** [create_state ?name ~f ~bin_query ~bin_state ~bin_update ()] will create an
      [Rpc.State_rpc.t] with [name] if specified. *)
  val create_state
    :  ?name:string
    -> ?client_pushes_back:unit
    -> f:
         (worker_state:worker_state
          -> conn_state:connection_state
          -> 'query
          -> ('state * 'update Pipe.Reader.t) Deferred.t)
    -> bin_query:'query Bin_prot.Type_class.t
    -> bin_state:'state Bin_prot.Type_class.t
    -> bin_update:'update Bin_prot.Type_class.t
    -> unit
    -> (worker, 'query, 'state * 'update Pipe.Reader.t) _function

  (** [of_async_rpc ~f rpc] is the analog to [create_rpc] but instead of creating an Rpc
      protocol, it uses the supplied one *)
  val of_async_rpc
    :  f:
         (worker_state:worker_state
          -> conn_state:connection_state
          -> 'query
          -> 'response Deferred.t)
    -> ('query, 'response) Rpc.Rpc.t
    -> (worker, 'query, 'response) _function

  (** [of_async_pipe_rpc ~f rpc] is the analog to [create_pipe] but instead of creating a
      Pipe rpc protocol, it uses the supplied one.

      Notice that [aborted] is not exposed. The pipe is closed upon aborted. *)
  val of_async_pipe_rpc
    :  f:
         (worker_state:worker_state
          -> conn_state:connection_state
          -> 'query
          -> 'response Pipe.Reader.t Deferred.t)
    -> ('query, 'response, Error.t) Rpc.Pipe_rpc.t
    -> (worker, 'query, 'response Pipe.Reader.t) _function

  (** [of_async_direct_pipe_rpc ~f rpc] is the analog to [create_direct_pipe] but instead
      of creating a Pipe rpc protocol, it uses the supplied one. *)
  val of_async_direct_pipe_rpc
    :  f:
         (worker_state:worker_state
          -> conn_state:connection_state
          -> 'query
          -> 'response Rpc.Pipe_rpc.Direct_stream_writer.t
          -> unit Deferred.t)
    -> ('query, 'response, Error.t) Rpc.Pipe_rpc.t
    -> (worker, 'query, 'response) _direct

  (** [of_async_one_way_rpc ~f rpc] is the analog to [create_one_way] but instead of
      creating a One_way rpc protocol, it uses the supplied one *)
  val of_async_one_way_rpc
    :  f:(worker_state:worker_state -> conn_state:connection_state -> 'query -> unit)
    -> 'query Rpc.One_way.t
    -> (worker, 'query, unit) _function

  (** [of_async_state_rpc ~f rpc] is the analog to [create_state] but instead of
      creating a State rpc protocol, it uses the supplied one *)
  val of_async_state_rpc
    :  f:
         (worker_state:worker_state
          -> conn_state:connection_state
          -> 'query
          -> ('state * 'update Pipe.Reader.t) Deferred.t)
    -> ('query, 'state, 'update, Error.t) Rpc.State_rpc.t
    -> (worker, 'query, 'state * 'update Pipe.Reader.t) _function
end

(** Specification for the creation of a worker type *)
module type Worker_spec = sig
  type ('worker, 'query, 'response) _function
  type ('worker, 'query, 'response) _direct

  (** A type to encapsulate all the functions that can be run on this worker. Using a
      record type here is often the most convenient and readable. *)
  type 'worker functions

  (** State associated with each [Worker.t]. If this state is mutable, you must
      think carefully when making multiple connections to the same spawned worker. *)
  module Worker_state : sig
    type t
    type init_arg [@@deriving bin_io]
  end

  (** State associated with each connection to a [Worker.t] *)
  module Connection_state : sig
    type t
    type init_arg [@@deriving bin_io]
  end

  (** The functions that can be run on this worker type *)
  module Functions
    (C : Creator
           with type worker_state = Worker_state.t
            and type connection_state = Connection_state.t
            and type ('w, 'q, 'r) _function := ('w, 'q, 'r) _function
            and type ('w, 'q, 'r) _direct := ('w, 'q, 'r) _direct) :
    Functions
      with type worker := C.worker
       and type 'a functions := 'a functions
       and type worker_state := Worker_state.t
       and type worker_state_init_arg := Worker_state.init_arg
       and type connection_state := Connection_state.t
       and type connection_state_init_arg := Connection_state.init_arg
end

(** An RPC backend. Defines how to create RPC servers and clients for a given protocol.
    Currently, we have two backends, one in each of rpc_parallel_unauthenticated and
    rpc_parallel_krb. These backends define how to create and connect to unencrypted and
    kerberized RPC servers, respectively. Unless you are implementing your own
    rpc_parallel backend, you should use one of the two libraries above instead of
    creating your own modules with this signature. *)
module type Backend = sig
  (** The name of the backend -- e.g. "Unauthenticated Async RPC". Must be globally
      unique. Used for error messages if someone initializes Rpc_parallel multiple times
      with different backends. *)
  val name : string

  module Settings : sig
    (** Additional settings needed to create or connect to an RPC server for this backend.
        For unecrypted RPC, this is unit. For kerberized RPC, this is the client and
        server krb modes. *)
    type t [@@deriving bin_io, sexp]
  end

  val serve
    :  ?max_message_size:int
    -> ?buffer_age_limit:Writer.buffer_age_limit
    -> ?handshake_timeout:Time_float.Span.t
    -> ?heartbeat_config:Rpc.Connection.Heartbeat_config.t
    -> implementations:'a Rpc.Implementations.t
    -> initial_connection_state:(Socket.Address.Inet.t -> Rpc.Connection.t -> 'a)
    -> where_to_listen:Tcp.Where_to_listen.inet
    -> Settings.t
    -> (Socket.Address.Inet.t, int) Tcp.Server.t Deferred.t

  val with_client
    :  ?implementations:Rpc.Connection.Client_implementations.t
    -> ?max_message_size:int
    -> ?buffer_age_limit:Writer.buffer_age_limit
    -> ?handshake_timeout:Time_float.Span.t
    -> ?heartbeat_config:Rpc.Connection.Heartbeat_config.t
    -> Settings.t
    -> Socket.Address.Inet.t Tcp.Where_to_connect.t
    -> (Rpc.Connection.t -> 'a Deferred.t)
    -> 'a Or_error.t Deferred.t

  val client
    :  ?implementations:Rpc.Connection.Client_implementations.t
    -> ?max_message_size:int
    -> ?buffer_age_limit:Writer.buffer_age_limit
    -> ?handshake_timeout:Time_float.Span.t
    -> ?heartbeat_config:Rpc.Connection.Heartbeat_config.t
    -> ?description:Info.t
    -> Settings.t
    -> Socket.Address.Inet.t Tcp.Where_to_connect.t
    -> Rpc.Connection.t Or_error.t Deferred.t
end

(** A backend packed together with its settings. Unless you are implementing your own
    rpc_parallel backend, you should use either rpc_parallel_krb or
    rpc_parallel_unauthenticated instead of constructing values of this type *)
module type Backend_and_settings = sig
  type t = T : (module Backend with type Settings.t = 'a) * 'a -> t
end

module type Parallel = sig
  module Function : Function
  module Backend_and_settings : Backend_and_settings

  module type Backend = Backend

  module type Worker =
    Worker
      with type ('w, 'q, 'r) _function := ('w, 'q, 'r) Function.t
       and type 'w _id_direct := 'w Function.Direct_pipe.Id.t

  module type Functions = Functions

  module type Creator =
    Creator
      with type ('w, 'q, 'r) _function := ('w, 'q, 'r) Function.t
       and type ('w, 'q, 'r) _direct := ('w, 'q, 'r) Function.Direct_pipe.t

  module type Worker_spec =
    Worker_spec
      with type ('w, 'q, 'r) _function := ('w, 'q, 'r) Function.t
       and type ('w, 'q, 'r) _direct := ('w, 'q, 'r) Function.Direct_pipe.t

  (** module Worker = Make(T)

      The [Worker] module has specialized functions to spawn workers and run functions on
      workers. *)
  module Make (S : Worker_spec) :
    Worker
      with type 'a functions := 'a S.functions
       and type worker_state_init_arg := S.Worker_state.init_arg
       and type connection_state_init_arg := S.Connection_state.init_arg

  (** [start_app command] should be called from the top-level in order to start the
      parallel application. This function will parse certain environment variables and
      determine whether to start as a master or a worker.

      [rpc_max_message_size], [rpc_handshake_timeout], [rpc_heartbeat_config] can be used
      to alter the rpc defaults. These rpc settings will be used for all connections.
      This can be useful if you have long async jobs.

      [when_parsing_succeeds] and [complete_subcommands] will be passed to [Command.run]
      in the master process. *)
  val start_app
    :  ?rpc_max_message_size:int
    -> ?rpc_buffer_age_limit:Writer.buffer_age_limit
    -> ?rpc_handshake_timeout:Time_float.Span.t
    -> ?rpc_heartbeat_config:Rpc.Connection.Heartbeat_config.t
    -> ?when_parsing_succeeds:(unit -> unit)
    -> ?complete_subcommands:
         (path:string list -> part:string -> string list list -> string list option)
    -> ?add_validate_parsing_flag:bool
    -> Backend_and_settings.t
       (** Use rpc_parallel_krb or rpc_parallel_unauthenticated to avoid having to manually
        construct a custom Backend *)
    -> Command.t
    -> unit

  (** Use [State.get] to query whether the current process has been initialized as an rpc
      parallel master ([start_app] or [init_master_exn] has been called). We return a
      [State.t] rather than a [bool] so that you can require evidence at the type level.
      If you want to certify, as a precondition, for some function that [start_app] was
      used, require a [State.t] as an argument. If you don't need the [State.t] anymore,
      just pattern match on it. *)
  module State : sig
    type t = private [< `started ]

    val get : unit -> t option
  end

  module For_testing : sig
    (** [initialize [%here]] must be called at the top level of any files that have inline
        or expect tests that use [Rpc_parallel]. Further, these calls must come before the
        definitions of the tests, but after the definitions of any workers used in the
        tests.

        For example:

        {[

          let () = Rpc_parallel.For_testing.initialize [%here]

          let%expect_test "" =
            run_code_with_rpc_parallel ();
            [%expect {| output |}]
          ;;

        ]}
    *)
    val initialize
      :  Backend_and_settings.t
         (** Use rpc_parallel_krb or rpc_parallel_unauthenticated to avoid having to manually
          construct a custom Backend *)
      -> Source_code_position.t
      -> unit
  end

  (** If you want more direct control over your executable, you can use the [Expert]
      module instead of [start_app]. If you use [Expert], you are responsible for starting
      the master and worker rpc servers. [worker_command_args] will be the arguments sent
      to each spawned worker. Running your executable with these args must follow a code
      path that calls [worker_init_before_async_exn] and then [start_worker_server_exn].
      An easy way to do this is to use [worker_command]. *)
  module Expert : sig
    (** [start_master_server_exn] must be called in the single master process. It is
        necessary to be able to spawn workers. Raises if the process was spawned.

        If [pass_name] is [false], the [?name] argument to spawned workers will not be
        propagated into the worker's command line. This override is only needed to support
        the "deprecated option" for implementing worker commands described below. *)
    val start_master_server_exn
      :  ?rpc_max_message_size:int
      -> ?rpc_buffer_age_limit:Writer.buffer_age_limit
      -> ?rpc_handshake_timeout:Time_float.Span.t
      -> ?rpc_heartbeat_config:Rpc.Connection.Heartbeat_config.t
      -> ?pass_name:bool (** default: true *)
      -> Backend_and_settings.t
         (** Use rpc_parallel_krb or rpc_parallel_unauthenticated to avoid having to manually
          construct a custom Backend *)
      -> worker_command_args:string list
      -> unit
      -> unit

    (** You have two options for implementing the worker process, a simple one and a
        deprecated one.

        Simple option: just make sure [worker_command] is somewhere in the command
        hierarchy of the same executable in which [start_master_server_exn] is called,
        with a subcommand path equal to [worker_command_args]. It is possible for multiple
        calls to [start_master_server_exn] to share the same [worker_command_args].

        Deprecated option: implement something at least as complicated yourself using
        [worker_init_before_async_exn] and [start_worker_server_exn]. This option may go
        away in the future. *)

    val worker_command
      :  (module Backend)
         (** Use rpc_parallel_krb or rpc_parallel_unauthenticated to avoid having to manually
          construct a custom Backend *)
      -> Command.t

    module Worker_env : sig
      type t
    end

    (** [worker_init_before_async_exn] must be called in a spawned worker process before
        the async scheduler has started. You must not read from stdin before this function
        call.

        This has the side effect of calling [chdir]. *)
    val worker_init_before_async_exn : unit -> Worker_env.t

    (** [start_worker_server_exn] must be called in each spawned process. It is illegal to
        call both [start_master_server_exn] and [start_worker_server_exn] in the same
        process. Raises if the process was not spawned.

        This has the side effect of scheduling a job that completes the daemonization of
        this process (if the process should daemonize). This includes redirecting stdout
        and stderr according to [redirect_stdout] and [redirect_stderr]. All writes to
        stdout before this job runs are blackholed. All writes to stderr before this job
        runs are redirected to the spawning process's stderr. *)
    val start_worker_server_exn
      :  (module Backend)
         (** Use rpc_parallel_krb or rpc_parallel_unauthenticated to avoid having to manually
          construct a custom Backend *)
      -> Worker_env.t
      -> unit
  end
end
