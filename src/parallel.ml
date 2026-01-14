open Core
open Async
open Parallel_intf
module Worker_type_id = Utils.Worker_type_id
module Worker_id = Utils.Worker_id

(* All processes start a "master" rpc server. This is a server that has two
   implementations:

   (1) Register - Spawned workers say hello to the spawner
   (2) Handle_exn - Spawned workers send exceptions to the spawner

   Processes can also start "worker" rpc servers (in process using [serve] or out of
   process using [spawn]). A "worker" rpc server has all the user defined implementations
   as well as:

   (1) Init_worker_state - Spawner sends the worker init argument
   (2) Init_connection_state - Connector sends the connection state init argument
   (3) Shutdown, Close_server, Async_log, etc.

   The handshake protocol for spawning a worker:

   (Master) - SSH and start running executable (Worker) - Start server, send
   [Register_rpc] with its host and port (Master) - Connect to worker, send
   [Init_worker_state_rpc] (Worker) - Do initialization (ensure we have daemonized first)
   (Master) - Finally, return a [Worker.t] to the caller
*)

module Worker_implementations = struct
  type t =
    | T :
        ('state, 'connection_state) Utils.Internal_connection_state.t Rpc.Implementation.t
          list
        -> t
end

(* Applications of the [Make()] functor have the side effect of populating an
   [implementations] list which subsequently adds an entry for that worker type id to the
   [worker_implementations]. *)
let worker_implementations = Worker_type_id.Table.create ~size:1 ()

module Worker_command_args = struct
  type t =
    | Add_master_pid
    | User_supplied of
        { args : string list
        ; pass_name : bool
        }
  [@@deriving sexp]
end

module Server_with_rpc_settings = struct
  type t =
    { server : (Socket.Address.Inet.t, int) Tcp.Server.t
    ; rpc_settings : Rpc_settings.t
    }
end

(* Functions that are implemented by all workers *)
module Shutdown_rpc = struct
  let rpc = Rpc.One_way.create ~name:"shutdown_rpc" ~version:0 ~bin_msg:Unit.bin_t
end

module Close_server_rpc = struct
  let rpc = Rpc.One_way.create ~name:"close_server_rpc" ~version:0 ~bin_msg:Unit.bin_t
end

module Worker_server_rpc_settings_rpc = struct
  let rpc =
    Rpc.Rpc.create
      ~name:"worker_server_rpc_settings_rpc"
      ~version:0
      ~bin_query:Unit.bin_t
      ~bin_response:Rpc_settings.bin_t
      ~include_in_error_count:Only_on_exn
  ;;
end

module Async_log_rpc = struct
  let rpc =
    Rpc.Pipe_rpc.create
      ~name:"async_log_rpc"
      ~version:0
      ~bin_query:Unit.bin_t
      ~bin_response:Log.Message.Stable.V2.bin_t
      ~bin_error:Error.bin_t
      ()
  ;;
end

module Function = struct
  module Rpc_id = Unique_id.Int ()

  let maybe_generate_name ~prefix ~name =
    match name with
    | None -> sprintf "%s_%s" prefix (Rpc_id.to_string (Rpc_id.create ()))
    | Some n -> n
  ;;

  module Function_piped = struct
    type ('worker, 'query, 'response) t = ('query, 'response, Error.t) Rpc.Pipe_rpc.t

    let make_impl ~monitor ~f protocol =
      Rpc.Pipe_rpc.implement
        protocol
        (fun ((_conn : Rpc.Connection.t), internal_conn_state) arg ->
          let { Utils.Internal_connection_state.conn_state; worker_state; _ } =
            Set_once.get_exn internal_conn_state
          in
          Utils.try_within ~monitor (fun () -> f ~worker_state ~conn_state arg))
        ~leave_open_on_exception:true
    ;;

    let make_direct_impl ~monitor ~f protocol =
      Rpc.Pipe_rpc.implement_direct
        protocol
        (fun ((_conn : Rpc.Connection.t), internal_conn_state) arg writer ->
          let { Utils.Internal_connection_state.conn_state; worker_state; _ } =
            Set_once.get_exn internal_conn_state
          in
          Utils.try_within ~monitor (fun () -> f ~worker_state ~conn_state arg writer))
        ~leave_open_on_exception:true
    ;;

    let make_proto ~name ~bin_input ~bin_output ~client_pushes_back =
      let name = maybe_generate_name ~prefix:"rpc_parallel_piped" ~name in
      Rpc.Pipe_rpc.create
        ?client_pushes_back
        ~name
        ~version:0
        ~bin_query:bin_input
        ~bin_response:bin_output
        ~bin_error:Error.bin_t
        ()
    ;;
  end

  module Function_state = struct
    type ('worker, 'query, 'state, 'update) t =
      ('query, 'state, 'update, Error.t) Rpc.State_rpc.t

    let make_impl ~monitor ~f protocol =
      Rpc.State_rpc.implement
        protocol
        (fun ((_conn : Rpc.Connection.t), internal_conn_state) arg ->
          let { Utils.Internal_connection_state.conn_state; worker_state; _ } =
            Set_once.get_exn internal_conn_state
          in
          Utils.try_within ~monitor (fun () -> f ~worker_state ~conn_state arg))
        ~leave_open_on_exception:true
    ;;

    let make_proto ~name ~bin_query ~bin_state ~bin_update ~client_pushes_back =
      let name = maybe_generate_name ~prefix:"rpc_parallel_state" ~name in
      Rpc.State_rpc.create
        ?client_pushes_back
        ~name
        ~version:0
        ~bin_query
        ~bin_state
        ~bin_update
        ~bin_error:Error.bin_t
        ()
    ;;
  end

  module Function_plain = struct
    type ('worker, 'query, 'response) t = ('query, 'response) Rpc.Rpc.t

    let make_impl ~monitor ~f protocol =
      Rpc.Rpc.implement
        protocol
        (fun ((_conn : Rpc.Connection.t), internal_conn_state) arg ->
           let { Utils.Internal_connection_state.conn_state; worker_state; _ } =
             Set_once.get_exn internal_conn_state
           in
           (* We want to raise any exceptions from [f arg] to the current monitor (handled
              by Rpc) so the caller can see it. Additional exceptions will be handled by
              the specified monitor *)
           Utils.try_within_exn ~monitor (fun () -> f ~worker_state ~conn_state arg))
    ;;

    let make_proto ~name ~bin_input ~bin_output =
      let name = maybe_generate_name ~prefix:"rpc_parallel_plain" ~name in
      Rpc.Rpc.create
        ~name
        ~version:0
        ~bin_query:bin_input
        ~bin_response:bin_output
        ~include_in_error_count:Only_on_exn
    ;;
  end

  module Function_reverse_piped = struct
    module Id = Unique_id.Int ()

    type ('worker, 'query, 'update, 'response, 'in_progress) t =
      { worker_rpc : ('query * Id.t, 'response) Rpc.Rpc.t
      ; master_rpc : (Id.t, 'update, Error.t) Rpc.Pipe_rpc.t
      ; master_in_progress : 'in_progress Id.Table.t
      }

    let make_worker_impl ~monitor ~f t =
      Rpc.Rpc.implement t.worker_rpc (fun (conn, internal_conn_state) (arg, id) ->
        let { Utils.Internal_connection_state.conn_state; worker_state; _ } =
          Set_once.get_exn internal_conn_state
        in
        Utils.try_within_exn ~monitor (fun () ->
          match%bind Rpc.Pipe_rpc.dispatch t.master_rpc conn id with
          | Ok (Ok (updates, (_ : Rpc.Pipe_rpc.Metadata.t))) ->
            f ~worker_state ~conn_state arg updates
          | Ok (Error error) | Error error -> Error.raise error))
    ;;

    let make_master t ~implement ~ok ~error =
      implement t.master_rpc (fun () id ->
        match Hashtbl.find_and_remove t.master_in_progress id with
        | Some in_progress -> ok in_progress
        | None ->
          [%message
            "Bug in Rpc_parallel: reverse pipe master implementation not found"
              (id : Id.t)
              (Rpc.Pipe_rpc.name t.master_rpc : string)]
          |> Deferred.Or_error.error_s
          |> error)
    ;;

    let make_master_impl t =
      make_master
        t
        ~implement:(Rpc.Pipe_rpc.implement ~leave_open_on_exception:true)
        ~ok:Deferred.Or_error.return
        ~error:Fn.id
    ;;

    let make_master_impl_direct t =
      make_master
        t
        ~implement:(Rpc.Pipe_rpc.implement_direct ~leave_open_on_exception:true)
        ~ok:Fn.id
        ~error:const
    ;;

    let make_proto ~name ~bin_query ~bin_update ~bin_response ~client_pushes_back =
      let name = maybe_generate_name ~prefix:"rpc_parallel_reverse_piped" ~name in
      let worker_rpc =
        let module With_id = struct
          type 'a t = 'a * Id.t [@@deriving bin_io]
        end
        in
        Rpc.Rpc.create
          ~name
          ~version:0
          ~bin_query:(With_id.bin_t bin_query)
          ~bin_response
          ~include_in_error_count:Only_on_exn
      in
      let master_rpc =
        Rpc.Pipe_rpc.create
          ?client_pushes_back
          ~name
          ~version:0
          ~bin_query:Id.bin_t
          ~bin_response:bin_update
          ~bin_error:Error.bin_t
          ()
      in
      let master_in_progress = Id.Table.create () in
      { worker_rpc; master_rpc; master_in_progress }
    ;;
  end

  module Function_one_way = struct
    type ('worker, 'query) t = 'query Rpc.One_way.t

    let make_impl ~monitor ~f protocol =
      Rpc.One_way.implement
        protocol
        (fun ((_conn : Rpc.Connection.t), internal_conn_state) arg ->
          let { Utils.Internal_connection_state.conn_state; worker_state; _ } =
            Set_once.get_exn internal_conn_state
          in
          don't_wait_for
            (* Even though [f] returns [unit], we want to use [try_within_exn] so if it
               starts any background jobs we won't miss the exceptions *)
            (Utils.try_within_exn ~monitor (fun () ->
               f ~worker_state ~conn_state arg |> return)))
        ~on_exception:Close_connection
    ;;

    let make_proto ~name ~bin_input =
      let name =
        match name with
        | None -> sprintf "rpc_parallel_one_way_%s" (Rpc_id.to_string (Rpc_id.create ()))
        | Some n -> n
      in
      Rpc.One_way.create ~name ~version:0 ~bin_msg:bin_input
    ;;
  end

  module Id_directly_piped = struct
    type 'worker t = T : _ Rpc.Pipe_rpc.t * Rpc.Pipe_rpc.Id.t -> 'worker t

    let abort (T (proto, id)) connection = Rpc.Pipe_rpc.abort proto connection id
  end

  type ('worker, 'query, 'response) t_internal =
    | Plain of ('worker, 'query, 'response) Function_plain.t
    | Piped :
        ('worker, 'query, 'response) Function_piped.t
        * ('r, 'response Pipe.Reader.t) Type_equal.t
        -> ('worker, 'query, 'r) t_internal
    | State :
        ('worker, 'query, 'state, 'update) Function_state.t
        * ('r, 'state * 'update Pipe.Reader.t) Type_equal.t
        -> ('worker, 'query, 'r) t_internal
    | Directly_piped :
        ('worker, 'query, 'response) Function_piped.t
        -> ( 'worker
             , 'query
               * ('response Rpc.Pipe_rpc.Pipe_message.t -> Rpc.Pipe_rpc.Pipe_response.t)
             , 'worker Id_directly_piped.t )
             t_internal
    | One_way : ('worker, 'query) Function_one_way.t -> ('worker, 'query, unit) t_internal
    | Reverse_piped :
        ('worker, 'query, 'update, 'response, 'in_progress) Function_reverse_piped.t
        * ('q, 'query * 'in_progress) Type_equal.t
        -> ('worker, 'q, 'response) t_internal

  type ('worker, 'query, +'response) t =
    | T :
        ('query -> 'query_internal)
        * ('worker, 'query_internal, 'response_internal) t_internal
        * ('response_internal -> 'response)
        -> ('worker, 'query, 'response) t

  module Direct_pipe = struct
    module Id = Id_directly_piped

    type nonrec ('worker, 'query, 'response) t =
      ( 'worker
        , 'query * ('response Rpc.Pipe_rpc.Pipe_message.t -> Rpc.Pipe_rpc.Pipe_response.t)
        , 'worker Id.t )
        t
  end

  let map (T (q, i, r)) ~f = T (q, i, Fn.compose f r)
  let contra_map (T (q, i, r)) ~f = T (Fn.compose q f, i, r)

  let create_rpc ~monitor ~name ~f ~bin_input ~bin_output =
    let proto = Function_plain.make_proto ~name ~bin_input ~bin_output in
    let impl = Function_plain.make_impl ~monitor ~f proto in
    T (Fn.id, Plain proto, Fn.id), impl
  ;;

  let create_pipe ~monitor ~name ~f ~bin_input ~bin_output ~client_pushes_back =
    let proto =
      Function_piped.make_proto ~name ~bin_input ~bin_output ~client_pushes_back
    in
    let impl = Function_piped.make_impl ~monitor ~f proto in
    T (Fn.id, Piped (proto, Type_equal.T), Fn.id), impl
  ;;

  let create_state ~monitor ~name ~f ~bin_query ~bin_state ~bin_update ~client_pushes_back
    =
    let proto =
      Function_state.make_proto
        ~name
        ~bin_query
        ~bin_state
        ~bin_update
        ~client_pushes_back
    in
    let impl = Function_state.make_impl ~monitor ~f proto in
    T (Fn.id, State (proto, Type_equal.T), Fn.id), impl
  ;;

  let create_direct_pipe ~monitor ~name ~f ~bin_input ~bin_output ~client_pushes_back =
    let proto =
      Function_piped.make_proto ~name ~bin_input ~bin_output ~client_pushes_back
    in
    let impl = Function_piped.make_direct_impl ~monitor ~f proto in
    T (Fn.id, Directly_piped proto, Fn.id), impl
  ;;

  let create_one_way ~monitor ~name ~f ~bin_input =
    let proto = Function_one_way.make_proto ~name ~bin_input in
    let impl = Function_one_way.make_impl ~monitor ~f proto in
    T (Fn.id, One_way proto, Fn.id), impl
  ;;

  let reverse_pipe
    ~make_master_impl
    ~monitor
    ~name
    ~f
    ~bin_query
    ~bin_update
    ~bin_response
    ~client_pushes_back
    =
    let proto =
      Function_reverse_piped.make_proto
        ~name
        ~bin_query
        ~bin_update
        ~bin_response
        ~client_pushes_back
    in
    let worker_impl = Function_reverse_piped.make_worker_impl ~monitor ~f proto in
    let master_impl = make_master_impl proto in
    ( T (Fn.id, Reverse_piped (proto, Type_equal.T), Fn.id)
    , `Worker worker_impl
    , `Master master_impl )
  ;;

  let create_reverse_pipe ~monitor ~name ~f ~bin_query ~bin_update ~bin_response =
    reverse_pipe
      ~make_master_impl:Function_reverse_piped.make_master_impl
      ~monitor
      ~name
      ~f
      ~bin_query
      ~bin_update
      ~bin_response
  ;;

  let create_reverse_direct_pipe ~monitor ~name ~f ~bin_query ~bin_update ~bin_response =
    reverse_pipe
      ~make_master_impl:Function_reverse_piped.make_master_impl_direct
      ~monitor
      ~name
      ~f
      ~bin_query
      ~bin_update
      ~bin_response
  ;;

  let of_async_rpc ~monitor ~f proto =
    let impl = Function_plain.make_impl ~monitor ~f proto in
    T (Fn.id, Plain proto, Fn.id), impl
  ;;

  let of_async_pipe_rpc ~monitor ~f proto =
    let impl = Function_piped.make_impl ~monitor ~f proto in
    T (Fn.id, Piped (proto, Type_equal.T), Fn.id), impl
  ;;

  let of_async_state_rpc ~monitor ~f proto =
    let impl = Function_state.make_impl ~monitor ~f proto in
    T (Fn.id, State (proto, Type_equal.T), Fn.id), impl
  ;;

  let of_async_direct_pipe_rpc ~monitor ~f proto =
    let impl = Function_piped.make_direct_impl ~monitor ~f proto in
    T (Fn.id, Directly_piped proto, Fn.id), impl
  ;;

  let of_async_one_way_rpc ~monitor ~f proto =
    let impl = Function_one_way.make_impl ~monitor ~f proto in
    T (Fn.id, One_way proto, Fn.id), impl
  ;;

  let handle_pipe_close_error on_pipe_rpc_close_error metadata =
    Option.iter on_pipe_rpc_close_error ~f:(fun callback ->
      don't_wait_for
        (match%bind Rpc.Pipe_rpc.close_reason metadata with
         | Closed_locally | Closed_remotely -> return ()
         | Error err ->
           callback err;
           return ()))
  ;;

  let run_internal
    (type worker query response)
    ?on_pipe_rpc_close_error
    (t_internal : (worker, query, response) t_internal)
    connection
    ~(arg : query)
    : response Or_error.t Deferred.t
    =
    match t_internal with
    | Plain proto -> Rpc.Rpc.dispatch proto connection arg
    | Piped (proto, Type_equal.T) ->
      let%map result = Rpc.Pipe_rpc.dispatch proto connection arg in
      Or_error.join result
      |> Or_error.map ~f:(fun (reader, metadata) ->
        handle_pipe_close_error on_pipe_rpc_close_error metadata;
        reader)
    | State (proto, Type_equal.T) ->
      let%map result = Rpc.State_rpc.dispatch proto connection arg in
      Or_error.join result
      |> Or_error.map ~f:(fun (state, reader, metadata) ->
        handle_pipe_close_error on_pipe_rpc_close_error metadata;
        state, reader)
    | One_way proto -> Rpc.One_way.dispatch proto connection arg |> return
    | Reverse_piped ({ worker_rpc; master_rpc = _; master_in_progress }, Type_equal.T) ->
      let query, updates = arg in
      let key = Function_reverse_piped.Id.create () in
      Hashtbl.add_exn master_in_progress ~key ~data:updates;
      let%map result = Rpc.Rpc.dispatch worker_rpc connection (query, key) in
      Hashtbl.remove master_in_progress key;
      result
    | Directly_piped proto ->
      let arg, f = arg in
      let%map result = Rpc.Pipe_rpc.dispatch_iter proto connection arg ~f in
      Or_error.join result |> Or_error.map ~f:(fun id -> Id_directly_piped.T (proto, id))
  ;;

  let run ?on_pipe_rpc_close_error (T (query_f, t_internal, response_f)) connection ~arg =
    run_internal ?on_pipe_rpc_close_error t_internal connection ~arg:(query_f arg)
    >>| Or_error.map ~f:response_f
  ;;

  let async_log = T (Fn.id, Piped (Async_log_rpc.rpc, Type_equal.T), Fn.id)
  let close_server = T (Fn.id, One_way Close_server_rpc.rpc, Fn.id)

  module For_internal_testing = struct
    let worker_server_rpc_settings =
      T (Fn.id, Plain Worker_server_rpc_settings_rpc.rpc, Fn.id)
    ;;
  end
end

module Daemonize_args = struct
  type args =
    { umask : int option
    ; redirect_stderr : Fd_redirection.t
    ; redirect_stdout : Fd_redirection.t
    }
  [@@deriving sexp]

  type t =
    [ `Don't_daemonize
    | `Daemonize of args
    ]
  [@@deriving sexp]
end

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

module Backend_and_settings = struct
  type t = T : (module Backend with type Settings.t = 'a) * 'a -> t

  let backend (T ((module Backend), _)) = (module Backend : Backend)
end

module Backend : sig
  module Settings : sig
    type t [@@deriving bin_io, sexp]

    val with_backend : t -> (module Backend) -> Backend_and_settings.t
  end

  val set_once_and_get_settings : Backend_and_settings.t -> Settings.t
  val assert_already_initialized_with_same_backend : (module Backend) -> unit

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
end = struct
  module Settings = struct
    type t = Sexp.t [@@deriving bin_io, sexp]

    let with_backend t (module Backend : Backend) =
      let t = [%of_sexp: Backend.Settings.t] t in
      Backend_and_settings.T
        ((module Backend : Backend with type Settings.t = Backend.Settings.t), t)
    ;;
  end

  let backend = Set_once.create ()

  let set_once_and_get_settings
    (Backend_and_settings.T ((module Backend), backend_settings))
    : Settings.t
    =
    Set_once.set_exn backend (module Backend : Backend);
    [%sexp_of: Backend.Settings.t] backend_settings
  ;;

  let get_backend_and_settings_exn backend_settings =
    let backend = Set_once.get_exn backend in
    Settings.with_backend backend_settings backend
  ;;

  let assert_already_initialized_with_same_backend (module Other_backend : Backend) =
    let (module Backend : Backend) = Set_once.get_exn backend in
    let backend = Backend.name in
    let other_backend = Other_backend.name in
    if not (String.equal backend other_backend)
    then
      raise_s
        [%message
          "Rpc_parallel was illegally initialized with two different backends in the \
           same executable"
            ~backend
            ~other_backend]
  ;;

  let serve
    ?max_message_size
    ?buffer_age_limit
    ?handshake_timeout
    ?heartbeat_config
    ~implementations
    ~initial_connection_state
    ~where_to_listen
    backend_settings
    =
    let (Backend_and_settings.T ((module Backend), backend_settings)) =
      get_backend_and_settings_exn backend_settings
    in
    Backend.serve
      ?max_message_size
      ?buffer_age_limit
      ?handshake_timeout
      ?heartbeat_config
      ~implementations
      ~initial_connection_state
      ~where_to_listen
      backend_settings
  ;;

  let with_client
    ?implementations
    ?max_message_size
    ?buffer_age_limit
    ?handshake_timeout
    ?heartbeat_config
    backend_settings
    where_to_connect
    f
    =
    let (Backend_and_settings.T ((module Backend), backend_settings)) =
      get_backend_and_settings_exn backend_settings
    in
    Backend.with_client
      ?implementations
      ?max_message_size
      ?buffer_age_limit
      ?handshake_timeout
      ?heartbeat_config
      backend_settings
      where_to_connect
      f
  ;;

  let client
    ?implementations
    ?max_message_size
    ?buffer_age_limit
    ?handshake_timeout
    ?heartbeat_config
    ?description
    backend_settings
    where_to_connect
    =
    let (Backend_and_settings.T ((module Backend), backend_settings)) =
      get_backend_and_settings_exn backend_settings
    in
    Backend.client
      ?implementations
      ?max_message_size
      ?buffer_age_limit
      ?handshake_timeout
      ?heartbeat_config
      ?description
      backend_settings
      where_to_connect
  ;;
end

module Worker_config = struct
  type t =
    { worker_type : Worker_type_id.t
    ; worker_id : Worker_id.t
    ; name : string option
    ; master : Host_and_port.t
    ; app_rpc_settings : Rpc_settings.t
    ; backend_settings : Backend.Settings.t
    ; cd : string
    ; daemonize_args : Daemonize_args.t
    ; connection_timeout : Time_float.Span.t
    ; worker_command_args : Worker_command_args.t
    }
  [@@deriving fields ~getters, sexp]
end

module Worker_env = struct
  type t =
    { config : Worker_config.t
    ; maybe_release_daemon : unit -> unit
    }
  [@@deriving fields ~getters]
end

(* We want to make sure the [Rpc_settings] used on both the client and server side of all
   communication within a single rpc_parallel application are the same. To help prevent
   mistakes, we write small wrappers around the [Rpc.Connection] functions. *)
let rpc_connection_with_client
  backend_settings
  ~rpc_settings
  ?implementations
  where_to_connect
  f
  =
  let { Rpc_settings.max_message_size
      ; buffer_age_limit
      ; handshake_timeout
      ; heartbeat_config
      }
    =
    rpc_settings
  in
  Backend.with_client
    ?max_message_size
    ?buffer_age_limit
    ?handshake_timeout
    ?heartbeat_config
    ?implementations
    backend_settings
    where_to_connect
    f
;;

let rpc_connection_client backend_settings ~rpc_settings ?implementations where_to_connect
  =
  let { Rpc_settings.max_message_size
      ; buffer_age_limit
      ; handshake_timeout
      ; heartbeat_config
      }
    =
    rpc_settings
  in
  Backend.client
    ?max_message_size
    ?buffer_age_limit
    ?handshake_timeout
    ?heartbeat_config
    ?implementations
    backend_settings
    where_to_connect
;;

let start_server
  backend_settings
  ~rpc_settings
  ~where_to_listen
  ~implementations
  ~initial_connection_state
  =
  let { Rpc_settings.max_message_size
      ; buffer_age_limit
      ; handshake_timeout
      ; heartbeat_config
      }
    =
    rpc_settings
  in
  let implementations =
    Rpc.Implementations.create_exn
      ~implementations
      ~on_unknown_rpc:`Close_connection
      ~on_exception:Log_on_background_exn
  in
  let%bind server =
    Backend.serve
      ~implementations
      ~initial_connection_state
      ?max_message_size
      ?buffer_age_limit
      ?handshake_timeout
      ?heartbeat_config
      ~where_to_listen
      backend_settings
  in
  return { Server_with_rpc_settings.server; rpc_settings }
;;

module Heartbeater_master : sig
  type t [@@deriving bin_io]

  val create
    :  host_and_port:Host_and_port.t
    -> rpc_settings:Rpc_settings.t
    -> backend_settings:Backend.Settings.t
    -> t

  val connect_and_shutdown_on_disconnect_exn : t -> [ `Connected ] Deferred.t
end = struct
  type t =
    { host_and_port : Host_and_port.t
    ; rpc_settings : Rpc_settings.t
    ; backend_settings : Backend.Settings.t
    }
  [@@deriving bin_io]

  let create ~host_and_port ~rpc_settings ~backend_settings =
    { host_and_port; rpc_settings; backend_settings }
  ;;

  let connect_and_wait_for_disconnect_exn
    { host_and_port; rpc_settings; backend_settings }
    =
    match%map
      rpc_connection_client
        backend_settings
        ~rpc_settings
        (Tcp.Where_to_connect.of_host_and_port host_and_port)
    with
    | Error e -> raise (Error.to_exn e)
    | Ok conn ->
      `Connected
        (let%map reason = Rpc.Connection.close_reason ~on_close:`finished conn in
         `Disconnected reason)
  ;;

  let connect_and_shutdown_on_disconnect_exn heartbeater =
    let%bind (`Connected wait_for_disconnect) =
      connect_and_wait_for_disconnect_exn heartbeater
    in
    (wait_for_disconnect
     >>> fun (`Disconnected reason) ->
     [%log.error
       "Rpc_parallel: Heartbeater with master lost connection... Shutting down."
         (reason : Info.t)];
     Shutdown.shutdown 254);
    return `Connected
  ;;
end

(* All global state that is needed for a process to act as a master *)
type master_state =
  { (* The [Host_and_port.t] corresponding to one's own master Rpc server. *)
    my_server : Host_and_port.t Deferred.t lazy_t
      (* The rpc settings used universally for all rpc connections *)
  ; app_rpc_settings : Rpc_settings.t
  ; backend_settings : Backend.Settings.t
      (* Used to facilitate timeout of connecting to a spawned worker *)
  ; pending : Host_and_port.t Ivar.t Worker_id.Table.t
      (* Arguments used when spawning a new worker. *)
  ; worker_command_args : Worker_command_args.t
      (* Callbacks for spawned worker exceptions along with the monitor that was current
         when [spawn] was called *)
  ; on_failures : ((Error.t -> unit) * Monitor.t) Worker_id.Table.t
  }

(* All global state that is not specific to worker types is collected here *)
type worker_state =
  { (* Currently running worker servers in this process *)
    my_worker_servers : Server_with_rpc_settings.t Worker_id.Table.t
      (* To facilitate process creation cleanup. *)
  ; initialized : [ `Init_started of [ `Initialized ] Or_error.t Deferred.t ] Set_once.t
  }

type global_state =
  { as_master : master_state
  ; as_worker : worker_state
  }

(* Each running instance has the capability to work as a master. This state includes
   information needed to spawn new workers (my_server, my_rpc_settings, pending,
   worker_command_args), information to handle existing spawned workerd (on_failures), and
   information to handle worker servers that are running in process. *)
let global_state : global_state Set_once.t = Set_once.create ()

let get_state_exn () =
  match Set_once.get global_state with
  | None -> failwith "State should have been set already"
  | Some state -> state
;;

let get_master_state_exn () = (get_state_exn ()).as_master
let get_worker_state_exn () = (get_state_exn ()).as_worker

(* Rpcs implemented by master *)
module Register_rpc = struct
  type t = Worker_id.t * Host_and_port.t [@@deriving bin_io]

  type response =
    [ `Shutdown
    | `Registered
    ]
  [@@deriving bin_io]

  let rpc =
    Rpc.Rpc.create
      ~name:"register_worker_rpc"
      ~version:0
      ~bin_query:bin_t
      ~bin_response
      ~include_in_error_count:Only_on_exn
  ;;

  let implementation =
    Rpc.Rpc.implement rpc (fun () (id, worker_hp) ->
      let global_state = get_master_state_exn () in
      match Hashtbl.find global_state.pending id with
      | None ->
        (* We already returned a failure to the [spawn_worker] caller *)
        return `Shutdown
      | Some ivar ->
        Ivar.fill_exn ivar worker_hp;
        return `Registered)
  ;;
end

module Handle_exn_rpc = struct
  type t =
    { id : Worker_id.t
    ; name : string option
    ; error : Error.t
    }
  [@@deriving bin_io]

  let rpc =
    Rpc.Rpc.create
      ~name:"handle_worker_exn_rpc"
      ~version:0
      ~bin_query:bin_t
      ~bin_response:Unit.bin_t
      ~include_in_error_count:Only_on_exn
  ;;

  let implementation =
    Rpc.Rpc.implement rpc (fun () { id; name; error } ->
      let global_state = get_master_state_exn () in
      let on_failure, monitor = Hashtbl.find_exn global_state.on_failures id in
      let name = Option.value ~default:(Worker_id.to_string id) name in
      let error = Error.tag error ~tag:name in
      (* We can't just run [on_failure error] because this will be caught by the Rpc
         monitor for this implementation. *)
      Scheduler.within ~monitor (fun () -> on_failure error);
      return ())
  ;;
end

(* In order to spawn other workers, you must have an rpc server implementing
   [Register_rpc] and [Handle_exn_rpc] *)
let master_implementations =
  [ Register_rpc.implementation; Handle_exn_rpc.implementation ]
;;

(* Setup some global state necessary to act as a master (i.e. spawn workers). This
   includes starting an Rpc server with [master_implementations] *)
let init_master_state backend_and_settings ~rpc_settings ~worker_command_args =
  let backend_settings = Backend.set_once_and_get_settings backend_and_settings in
  match Set_once.get global_state with
  | Some _state -> failwith "Master state must not be set up twice"
  | None ->
    (* Use [size:1] so there is minimal top-level overhead linking with Rpc_parallel *)
    let pending = Worker_id.Table.create ~size:1 () in
    let on_failures = Worker_id.Table.create ~size:1 () in
    let my_worker_servers = Worker_id.Table.create ~size:1 () in
    (* Lazily start our master rpc server *)
    let my_server =
      lazy
        (let%map server =
           start_server
             backend_settings
             ~rpc_settings
             ~where_to_listen:Tcp.Where_to_listen.of_port_chosen_by_os
             ~implementations:master_implementations
             ~initial_connection_state:(fun _ _ -> ())
         in
         Host_and_port.create
           ~host:(Unix.gethostname ())
           ~port:(Tcp.Server.listening_on server.Server_with_rpc_settings.server))
    in
    let as_master =
      { my_server
      ; app_rpc_settings = rpc_settings
      ; backend_settings
      ; pending
      ; worker_command_args
      ; on_failures
      }
    in
    let as_worker = { my_worker_servers; initialized = Set_once.create () } in
    Set_once.set_exn global_state { as_master; as_worker }
;;

module Make (S : Worker_spec) = struct
  module Id = Utils.Worker_id

  type t =
    { host_and_port : Host_and_port.t
    ; rpc_settings : Rpc_settings.t
    ; backend_settings : Backend.Settings.t
    ; id : Worker_id.t
    ; name : string option
    }
  [@@deriving bin_io, sexp_of]

  type worker = t

  (* Internally we use [Worker_id.t] for all worker ids, but we want to expose an [Id]
     module that is specific to each worker. *)
  let id t = t.id
  let rpc_settings t = t.rpc_settings

  type worker_state =
    { (* A unique identifier for each application of the [Make] functor. Because we are
         running the same executable and this is supposed to run at the top level, the
         master and the workers agree on these ids *)
      type_ : Worker_type_id.t
        (* Persistent states associated with instances of this worker server *)
    ; states : S.Worker_state.t Worker_id.Table.t
        (* To facilitate cleanup in the [Shutdown_on.Connection_closed] case *)
    ; mutable client_has_connected : bool
        (* Build up a list of all implementations for this worker type *)
    ; mutable implementations :
        (S.Worker_state.t, S.Connection_state.t) Utils.Internal_connection_state.t
          Rpc.Implementation.t
          list
    ; mutable master_implementations : unit Rpc.Implementation.t list
    }

  let worker_state =
    { type_ = Worker_type_id.create ()
    ; states = Worker_id.Table.create ~size:1 ()
    ; client_has_connected = false
    ; implementations = []
    ; master_implementations = []
    }
  ;;

  (* Schedule all worker implementations in [Monitor.main] so no exceptions are lost.
     Async log automatically throws its exceptions to [Monitor.main] so we can't make our
     own local monitor. We detach [Monitor.main] and send exceptions back to the master. *)
  let monitor = Monitor.main

  (* Rpcs implemented by this worker type. The implementations for some must be below
     because User_functions is defined below (by supplying a [Creator] module) *)
  module Init_worker_state_rpc = struct
    module Worker_shutdown_on = struct
      type t =
        | Heartbeater_connection_timeout of Heartbeater_master.t
        | Connection_closed of { connection_timeout : Time_float.Span.t }
        | Called_shutdown_function
      [@@deriving bin_io]
    end

    type query =
      { worker_shutdown_on : Worker_shutdown_on.t
      ; worker : Worker_id.t (* The process that got spawned *)
      ; arg : S.Worker_state.init_arg
      }
    [@@deriving bin_io]

    let rpc =
      Rpc.Rpc.create
        ~name:(sprintf "worker_init_rpc_%s" (Worker_type_id.to_string worker_state.type_))
        ~version:0
        ~bin_query
        ~bin_response:Unit.bin_t
        ~include_in_error_count:Only_on_exn
    ;;
  end

  module Init_connection_state_rpc = struct
    type query =
      { worker_id : Worker_id.t
      ; worker_shutdown_on_disconnect : bool
      ; arg : S.Connection_state.init_arg
      }
    [@@deriving bin_io]

    let rpc =
      Rpc.Rpc.create
        ~name:
          (sprintf
             "set_connection_state_rpc_%s"
             (Worker_type_id.to_string worker_state.type_))
        ~version:0
        ~bin_query
        ~bin_response:Unit.bin_t
        ~include_in_error_count:Only_on_exn
    ;;
  end

  module Timeout : sig
    type t

    val after : Time_float.Span.t -> t
    val with_timeout : t -> 'a Deferred.t -> [ `Result of 'a | `Timeout ] Deferred.t
    val earlier : t -> t -> t
  end = struct
    type t = Time_float.t

    let after span = Time_float.add (Time_float.now ()) span

    let with_timeout t deferred =
      let event = Clock.Event.at t in
      choose
        [ choice deferred (fun result ->
            Clock.Event.abort_if_possible event ();
            `Result result)
        ; choice (Clock.Event.fired event) (fun (_ : _ Time_source.Event.Fired.t) ->
            `Timeout)
        ]
    ;;

    let earlier = Time_float.min
  end

  let kill_process process =
    Process.send_signal process Signal.term;
    let close_fds_and_wait () =
      let%map (_we_just_want_it_dead : Unix.Exit_or_signal.t) = Process.wait process
      and () = Writer.close (Process.stdin process)
      and () = Reader.close (Process.stdout process)
      and () = Reader.close (Process.stderr process) in
      ()
    in
    match%map Clock_ns.with_timeout (Time_ns.Span.of_sec 10.) (close_fds_and_wait ()) with
    | `Result () -> ()
    | `Timeout -> Process.send_signal process Signal.kill
  ;;

  let run_executable how ~env ~worker_command_args ~input ~timeout =
    let open Deferred.Or_error.Let_syntax in
    let%bind env = Utils.create_worker_env ~extra:env |> Deferred.return in
    let p = How_to_run.run how ~env ~worker_command_args in
    let%map p =
      match%bind.Deferred Timeout.with_timeout timeout p with
      | `Result p -> Deferred.return p
      | `Timeout ->
        don't_wait_for
          (let open Deferred.Let_syntax in
           match%bind p with
           | Error (_we_just_want_it_dead : Error.t) -> Deferred.unit
           | Ok p -> kill_process p);
        Deferred.Or_error.error_string "Timed out spawning worker"
    in
    Writer.write_sexp (Process.stdin p) input;
    p
  ;;

  module Function_creator = struct
    type nonrec worker = worker
    type connection_state = S.Connection_state.t
    type worker_state = S.Worker_state.t

    let with_add_impl f =
      let func, impl = f () in
      worker_state.implementations <- impl :: worker_state.implementations;
      func
    ;;

    let create_rpc ?name ~f ~bin_input ~bin_output () =
      with_add_impl (fun () ->
        Function.create_rpc ~monitor ~name ~f ~bin_input ~bin_output)
    ;;

    let create_pipe ?name ?client_pushes_back ~f ~bin_input ~bin_output () =
      with_add_impl (fun () ->
        Function.create_pipe ~monitor ~name ~f ~bin_input ~bin_output ~client_pushes_back)
    ;;

    let create_state ?name ?client_pushes_back ~f ~bin_query ~bin_state ~bin_update () =
      with_add_impl (fun () ->
        Function.create_state
          ~monitor
          ~name
          ~f
          ~bin_query
          ~bin_state
          ~bin_update
          ~client_pushes_back)
    ;;

    let create_direct_pipe ?name ?client_pushes_back ~f ~bin_input ~bin_output () =
      with_add_impl (fun () ->
        Function.create_direct_pipe
          ~monitor
          ~name
          ~f
          ~bin_input
          ~bin_output
          ~client_pushes_back)
    ;;

    let create_one_way ?name ~f ~bin_input () =
      with_add_impl (fun () -> Function.create_one_way ~monitor ~name ~f ~bin_input)
    ;;

    let reverse_pipe
      ~create_function
      ?name
      ?client_pushes_back
      ~f
      ~bin_query
      ~bin_update
      ~bin_response
      ()
      =
      let func, `Worker worker_impl, `Master master_impl =
        create_function
          ~monitor
          ~name
          ~f
          ~bin_query
          ~bin_update
          ~bin_response
          ~client_pushes_back
      in
      worker_state.implementations <- worker_impl :: worker_state.implementations;
      worker_state.master_implementations
      <- master_impl :: worker_state.master_implementations;
      func
    ;;

    let create_reverse_pipe
      ?name
      ?client_pushes_back
      ~f
      ~bin_query
      ~bin_update
      ~bin_response
      ()
      =
      reverse_pipe
        ~create_function:Function.create_reverse_pipe
        ?name
        ?client_pushes_back
        ~f
        ~bin_query
        ~bin_update
        ~bin_response
        ()
    ;;

    let create_reverse_direct_pipe
      ?name
      ?client_pushes_back
      ~f
      ~bin_query
      ~bin_update
      ~bin_response
      ()
      =
      reverse_pipe
        ~create_function:Function.create_reverse_direct_pipe
        ?name
        ?client_pushes_back
        ~f
        ~bin_query
        ~bin_update
        ~bin_response
        ()
    ;;

    let of_async_rpc ~f proto =
      with_add_impl (fun () -> Function.of_async_rpc ~monitor ~f proto)
    ;;

    let of_async_pipe_rpc ~f proto =
      with_add_impl (fun () -> Function.of_async_pipe_rpc ~monitor ~f proto)
    ;;

    let of_async_state_rpc ~f proto =
      with_add_impl (fun () -> Function.of_async_state_rpc ~monitor ~f proto)
    ;;

    let of_async_direct_pipe_rpc ~f proto =
      with_add_impl (fun () -> Function.of_async_direct_pipe_rpc ~monitor ~f proto)
    ;;

    let of_async_one_way_rpc ~f proto =
      with_add_impl (fun () -> Function.of_async_one_way_rpc ~monitor ~f proto)
    ;;
  end

  module User_functions = S.Functions (Function_creator)

  let functions = User_functions.functions

  let master_implementations : Rpc.Connection.Client_implementations.t =
    T
      { connection_state = const ()
      ; implementations =
          Rpc.Implementations.create_exn
            ~implementations:worker_state.master_implementations
            ~on_unknown_rpc:`Close_connection
            ~on_exception:Log_on_background_exn
      }
  ;;

  let serve worker_state_init_arg =
    match Hashtbl.find worker_implementations worker_state.type_ with
    | None ->
      failwith
        "Worker could not find RPC implementations. Make sure the Parallel.Make () \
         functor is applied in the worker. It is suggested to make this toplevel."
    | Some (Worker_implementations.T worker_implementations) ->
      let master_state = get_master_state_exn () in
      let rpc_settings = master_state.app_rpc_settings in
      let backend_settings = master_state.backend_settings in
      let%bind server =
        start_server
          backend_settings
          ~implementations:worker_implementations
          ~initial_connection_state:(fun _address connection ->
            connection, Set_once.create ())
          ~rpc_settings
          ~where_to_listen:Tcp.Where_to_listen.of_port_chosen_by_os
      in
      let id = Worker_id.create () in
      let host = Unix.gethostname () in
      let port = Tcp.Server.listening_on server.server in
      let global_state = get_worker_state_exn () in
      Hashtbl.add_exn global_state.my_worker_servers ~key:id ~data:server;
      let%map state = User_functions.init_worker_state worker_state_init_arg in
      Hashtbl.add_exn worker_state.states ~key:id ~data:state;
      { host_and_port = Host_and_port.create ~host ~port
      ; rpc_settings
      ; backend_settings
      ; id
      ; name = None
      }
  ;;

  module Connection = struct
    type t =
      { connection : Rpc.Connection.t
      ; worker_id : Id.t
      }
    [@@deriving fields ~getters, sexp_of]

    let close t = Rpc.Connection.close t.connection
    let close_finished t = Rpc.Connection.close_finished t.connection
    let close_reason t = Rpc.Connection.close_reason t.connection
    let is_closed t = Rpc.Connection.is_closed t.connection
    let underlying (t : t) = t.connection

    let client_aux
      ~worker_shutdown_on_disconnect
      { host_and_port; rpc_settings; id = worker_id; backend_settings; _ }
      init_arg
      =
      match%bind
        rpc_connection_client
          backend_settings
          ~rpc_settings
          ~implementations:master_implementations
          (Tcp.Where_to_connect.of_host_and_port host_and_port)
      with
      | Error error -> return (Error error)
      | Ok connection ->
        (match%bind
           Rpc.Rpc.dispatch
             Init_connection_state_rpc.rpc
             connection
             { worker_id; worker_shutdown_on_disconnect; arg = init_arg }
         with
         | Error e ->
           let%map () = Rpc.Connection.close connection in
           Error e
         | Ok () -> Deferred.Or_error.return { connection; worker_id })
    ;;

    let client = client_aux ~worker_shutdown_on_disconnect:false

    let client_with_worker_shutdown_on_disconnect =
      client_aux ~worker_shutdown_on_disconnect:true
    ;;

    let client_exn worker init_arg = client worker init_arg >>| Or_error.ok_exn

    let with_client worker init_arg ~f =
      client worker init_arg
      >>=? fun conn ->
      let%bind result = Monitor.try_with ~run:`Schedule ~rest:`Log (fun () -> f conn) in
      let%map () = close conn in
      Result.map_error result ~f:(fun exn -> Error.of_exn exn)
    ;;

    let run ?on_pipe_rpc_close_error t ~f ~arg =
      Function.run ?on_pipe_rpc_close_error f t.connection ~arg
    ;;

    let run_exn ?on_pipe_rpc_close_error t ~f ~arg =
      run ?on_pipe_rpc_close_error t ~f ~arg >>| Or_error.ok_exn
    ;;

    let abort t ~id = Function.Direct_pipe.Id.abort id t.connection
  end

  module Shutdown_on (M : T1) = struct
    type _ t =
      | Connection_closed :
          (connection_state_init_arg:S.Connection_state.init_arg
           -> Connection.t M.t Deferred.t)
            t
      | Heartbeater_connection_timeout : worker M.t Deferred.t t
      | Called_shutdown_function : worker M.t Deferred.t t

    let to_init_worker_state_rpc_query_arg (type a) (t : a t) ~connection_timeout =
      Staged.stage (fun ~heartbeater_master ->
        match t with
        | Heartbeater_connection_timeout ->
          Init_worker_state_rpc.Worker_shutdown_on.Heartbeater_connection_timeout
            heartbeater_master
        | Connection_closed ->
          Init_worker_state_rpc.Worker_shutdown_on.Connection_closed
            { connection_timeout }
        | Called_shutdown_function ->
          Init_worker_state_rpc.Worker_shutdown_on.Called_shutdown_function)
    ;;
  end

  type 'a with_spawn_args =
    ?how:How_to_run.t
    -> ?name:string
    -> ?env:(string * string) list
    -> ?connection_timeout:Time_float.Span.t
    -> ?spawn_timeout:Time_float.Span.t
    -> ?cd:string
    -> on_failure:(Error.t -> unit)
    -> 'a

  (* This timeout serves three purposes.

     [spawn] returns an error if:
     (1) the master hasn't gotten a register rpc from the spawned worker within
         [connection_timeout] of sending the register rpc.
     (2) a worker hasn't gotten its [init_arg] from the master within [connection_timeout]
         of sending the register rpc

     Additionally, if [~shutdown_on:Connection_closed] was used:

     (3) a worker will shut itself down if it doesn't get a connection from the master
     after spawn succeeded. *)
  let connection_timeout_default = sec 10.

  let decorate_error_if_running_test error =
    let tag =
      "You must call [Rpc_parallel.For_testing.initialize] at the top level before any \
       tests are defined. See lib/rpc_parallel/src/parallel_intf.ml for more \
       information."
    in
    if Core.am_running_test then Error.tag ~tag error else error
  ;;

  (* Specific environment variables that influence process execution that a child should
     inherit from the parent. Not copying can result in confusing behavior based on how
     RPC Parallel is configured because:
     - If no use of other machines is configured, all processes run locally and inherit
       the parent's full environment
     - If another machine is configured, all processes (even local ones) do not inherit
       the parent's environment *)
  let ocaml_env_vars_to_child = [ "OCAMLRUNPARAM"; "ASYNC_CONFIG" ]

  let ocaml_env_from_parent =
    lazy
      (List.filter_map ocaml_env_vars_to_child ~f:(fun var ->
         Option.map (Unix.getenv var) ~f:(fun value -> var, value)))
  ;;

  let spawn_process
    ~how
    ~env
    ~cd
    ~name
    ~timeout
    ~daemonize_args
    ~configured_connection_timeout
    =
    let how = Option.value how ~default:How_to_run.local in
    let env =
      (* [force ocaml_env_from_parent] must come before [Option.value env ~default:[]] so
         any user-supplied [env] can override [ocaml_env_from_parent]. *)
      force ocaml_env_from_parent @ Option.value env ~default:[]
      |> List.fold ~init:String.Map.empty ~f:(fun acc (key, data) ->
        Map.set acc ~key ~data)
      |> Map.to_alist
    in
    let cd = Option.value cd ~default:"/" in
    (match Set_once.get global_state with
     | None ->
       let error =
         Error.of_string
           "You must initialize this process to run as a master before calling [spawn]. \
            Either use a top-level [start_app] call or use the [Expert] module."
       in
       return (Error (decorate_error_if_running_test error))
     | Some global_state -> Deferred.Or_error.return global_state.as_master)
    >>=? fun global_state ->
    (* generate a unique identifier for this worker *)
    let id = Worker_id.create () in
    let%bind master_server = Lazy.force global_state.my_server in
    let input =
      { Worker_config.worker_type = worker_state.type_
      ; worker_id = id
      ; name
      ; master = master_server
      ; app_rpc_settings = global_state.app_rpc_settings
      ; backend_settings = global_state.backend_settings
      ; cd
      ; daemonize_args
      ; connection_timeout = configured_connection_timeout
      ; worker_command_args = global_state.worker_command_args
      }
      |> Worker_config.sexp_of_t
    in
    let pending_ivar = Ivar.create () in
    let worker_command_args =
      match global_state.worker_command_args with
      | Add_master_pid ->
        "RPC_PARALLEL_WORKER"
        :: sprintf !"child-of-%{Pid}@%s" (Unix.getpid ()) (Unix.gethostname ())
        :: Option.to_list name
      | User_supplied { args; pass_name } ->
        if pass_name then args @ Option.to_list name else args
    in
    Hashtbl.add_exn global_state.pending ~key:id ~data:pending_ivar;
    match%map run_executable how ~env ~worker_command_args ~input ~timeout with
    | Error _ as err ->
      Hashtbl.remove global_state.pending id;
      err
    | Ok process -> Ok (id, process)
  ;;

  let with_client worker ~f =
    let { host_and_port; rpc_settings; backend_settings; _ } = worker in
    rpc_connection_with_client
      backend_settings
      ~rpc_settings
      ~implementations:master_implementations
      (Tcp.Where_to_connect.of_host_and_port host_and_port)
      f
  ;;

  let shutdown worker =
    with_client worker ~f:(fun conn ->
      Rpc.One_way.dispatch Shutdown_rpc.rpc conn () |> return)
    >>| Or_error.join
  ;;

  let with_shutdown_on_error worker ~f =
    match%bind f () with
    | Ok _ as ret -> return ret
    | Error _ as ret ->
      let%bind (_ : unit Or_error.t) = shutdown worker in
      return ret
  ;;

  let wait_for_connection_and_initialize
    ~name
    ~timeout
    ~on_failure
    ~id
    ~worker_shutdown_on
    init_arg
    =
    let global_state = get_master_state_exn () in
    let pending_ivar = Hashtbl.find_exn global_state.pending id in
    (* Ensure that we got a register from the worker *)
    match%bind Timeout.with_timeout timeout (Ivar.read pending_ivar) with
    | `Timeout ->
      Hashtbl.remove global_state.pending id;
      Deferred.Or_error.error_string "Timed out getting connection from process"
    | `Result host_and_port ->
      Hashtbl.remove global_state.pending id;
      let worker =
        { host_and_port
        ; rpc_settings = global_state.app_rpc_settings
        ; backend_settings = global_state.backend_settings
        ; id
        ; name
        }
      in
      let%bind master_server = Lazy.force global_state.my_server in
      Hashtbl.add_exn
        global_state.on_failures
        ~key:worker.id
        ~data:(on_failure, Monitor.current ());
      with_shutdown_on_error worker ~f:(fun () ->
        match%map
          with_client worker ~f:(fun conn ->
            let heartbeater_master =
              Heartbeater_master.create
                ~host_and_port:master_server
                ~rpc_settings:global_state.app_rpc_settings
                ~backend_settings:global_state.backend_settings
            in
            let worker_shutdown_on = worker_shutdown_on ~heartbeater_master in
            Rpc.Rpc.dispatch
              Init_worker_state_rpc.rpc
              conn
              { worker_shutdown_on; worker = id; arg = init_arg })
        with
        | Error error ->
          Hashtbl.remove global_state.on_failures worker.id;
          Error error
        | Ok (Error e) ->
          Hashtbl.remove global_state.on_failures worker.id;
          Error e
        | Ok (Ok ()) -> Ok worker)
  ;;

  module Spawn_in_foreground_aux_result = struct
    type 'a t =
      ( 'a * Process.t
        , Error.t * [ `Worker_process of Unix.Exit_or_signal.t Deferred.t option ] )
        Result.t
  end

  module Spawn_in_foreground_aux_shutdown_on = Shutdown_on (Spawn_in_foreground_aux_result)

  let finalize_on_error ~finalize f =
    let%bind result = f () in
    match result with
    | Ok x -> return (Ok x)
    | Error e ->
      let finalized = finalize () in
      return (Error (e, finalized))
  ;;

  let default_spawn_timeout ~connection_timeout =
    Time_float.Span.scale connection_timeout 2.
  ;;

  let spawn_in_foreground_aux
    (type a)
    ?how
    ?name
    ?env
    ?connection_timeout
    ?spawn_timeout
    ?cd
    ~on_failure
    ~(shutdown_on : a Spawn_in_foreground_aux_shutdown_on.t)
    worker_state_init_arg
    : a
    =
    let open Deferred.Result.Let_syntax in
    let daemonize_args = `Don't_daemonize in
    let connection_timeout =
      Option.value connection_timeout ~default:connection_timeout_default
    in
    let worker_shutdown_on =
      Spawn_in_foreground_aux_shutdown_on.to_init_worker_state_rpc_query_arg
        shutdown_on
        ~connection_timeout
      |> unstage
    in
    let spawn_timeout =
      Timeout.after
        (Option.value spawn_timeout ~default:(default_spawn_timeout ~connection_timeout))
    in
    let with_spawned_worker ~f =
      let%bind id, process =
        spawn_process
          ~how
          ~env
          ~cd
          ~name
          ~daemonize_args
          ~timeout:spawn_timeout
          ~configured_connection_timeout:connection_timeout
        |> Deferred.Result.map_error ~f:(fun e -> e, `Worker_process None)
      in
      finalize_on_error
        ~finalize:(fun () ->
          let exit_or_signal =
            let open Deferred.Let_syntax in
            let%bind exit_status = Process.wait process in
            let%map () = Writer.close (Process.stdin process)
            and () = Reader.close (Process.stdout process)
            and () = Reader.close (Process.stderr process) in
            exit_status
          in
          `Worker_process (Some exit_or_signal))
        (fun () ->
          let%bind worker =
            wait_for_connection_and_initialize
              ~name
              ~timeout:(Timeout.earlier spawn_timeout (Timeout.after connection_timeout))
              ~on_failure
              ~id
              ~worker_shutdown_on
              worker_state_init_arg
          in
          f (worker, process))
    in
    match shutdown_on with
    | Heartbeater_connection_timeout -> with_spawned_worker ~f:return
    | Connection_closed ->
      fun ~connection_state_init_arg ->
        with_spawned_worker ~f:(fun (worker, process) ->
          (* If [Connection_state.init] raises, [client_internal] will close the Rpc
             connection, causing the worker to shutdown. *)
          let%bind conn =
            Connection.client_with_worker_shutdown_on_disconnect
              worker
              connection_state_init_arg
          in
          return (conn, process))
    | Called_shutdown_function -> with_spawned_worker ~f:return
  ;;

  module Spawn_in_foreground_result = struct
    type 'a t = ('a * Process.t) Or_error.t
  end

  module Spawn_in_foreground_shutdown_on = Shutdown_on (Spawn_in_foreground_result)

  let spawn_in_foreground
    (type a)
    ?how
    ?name
    ?env
    ?connection_timeout
    ?spawn_timeout
    ?cd
    ~on_failure
    ~(shutdown_on : a Spawn_in_foreground_shutdown_on.t)
    worker_state_init_arg
    : a
    =
    let spawn_in_foreground_aux
      (type a)
      ~(shutdown_on : a Spawn_in_foreground_aux_shutdown_on.t)
      : a
      =
      spawn_in_foreground_aux
        ?how
        ?name
        ?env
        ?connection_timeout
        ?spawn_timeout
        ?cd
        ~on_failure
        ~shutdown_on
        worker_state_init_arg
    in
    match shutdown_on with
    | Heartbeater_connection_timeout ->
      spawn_in_foreground_aux ~shutdown_on:Heartbeater_connection_timeout
      >>| Result.map_error ~f:fst
    | Connection_closed ->
      fun ~connection_state_init_arg ->
        spawn_in_foreground_aux ~shutdown_on:Connection_closed ~connection_state_init_arg
        >>| Result.map_error ~f:fst
    | Called_shutdown_function ->
      spawn_in_foreground_aux ~shutdown_on:Called_shutdown_function
      >>| Result.map_error ~f:fst
  ;;

  module Spawn_in_foreground_exn_result = struct
    type 'a t = 'a * Process.t
  end

  module Spawn_in_foreground_exn_shutdown_on = Shutdown_on (Spawn_in_foreground_exn_result)

  let spawn_in_foreground_exn
    (type a)
    ?how
    ?name
    ?env
    ?connection_timeout
    ?spawn_timeout
    ?cd
    ~on_failure
    ~(shutdown_on : a Spawn_in_foreground_exn_shutdown_on.t)
    init_arg
    : a
    =
    let open Spawn_in_foreground_exn_shutdown_on in
    match shutdown_on with
    | Connection_closed ->
      fun ~connection_state_init_arg ->
        spawn_in_foreground
          ?how
          ?name
          ?env
          ?connection_timeout
          ?spawn_timeout
          ?cd
          ~on_failure
          ~shutdown_on:Connection_closed
          init_arg
          ~connection_state_init_arg
        >>| ok_exn
    | Heartbeater_connection_timeout ->
      spawn_in_foreground
        ?how
        ?name
        ?env
        ?connection_timeout
        ?spawn_timeout
        ?cd
        ~on_failure
        ~shutdown_on:Heartbeater_connection_timeout
        init_arg
      >>| ok_exn
    | Called_shutdown_function ->
      spawn_in_foreground
        ?how
        ?name
        ?env
        ?connection_timeout
        ?spawn_timeout
        ?cd
        ~on_failure
        ~shutdown_on:Called_shutdown_function
        init_arg
      >>| ok_exn
  ;;

  let wait_for_daemonization_and_collect_stderr name process ~timeout =
    don't_wait_for
      (let%bind () = Writer.close (Process.stdin process) in
       let%bind () = Reader.close (Process.stdout process) in
       let worker_stderr = Reader.lines (Process.stderr process) in
       Pipe.iter worker_stderr ~f:(fun line ->
         let line' = sprintf "[WORKER %s STDERR]: %s\n" name line in
         Writer.write (Lazy.force Writer.stderr) line' |> return));
    match%map Timeout.with_timeout timeout (Process.wait process) with
    | `Timeout ->
      don't_wait_for (kill_process process);
      Or_error.error_string "Timed out waiting for worker to daemonize"
    | `Result (Ok ()) -> Ok ()
    | `Result (Error _ as exit_or_signal) ->
      let error =
        Error.create_s
          [%message
            "Worker process exited with error"
              ~_:(Unix.Exit_or_signal.to_string_hum exit_or_signal)]
      in
      Error (decorate_error_if_running_test error)
  ;;

  module Spawn_shutdown_on = Shutdown_on (Or_error)

  let spawn
    (type a)
    ?how
    ?name
    ?env
    ?connection_timeout
    ?spawn_timeout
    ?cd
    ~on_failure
    ?umask
    ~(shutdown_on : a Spawn_shutdown_on.t)
    ~redirect_stdout
    ~redirect_stderr
    worker_state_init_arg
    : a
    =
    let daemonize_args =
      `Daemonize { Daemonize_args.umask; redirect_stderr; redirect_stdout }
    in
    let connection_timeout =
      Option.value connection_timeout ~default:connection_timeout_default
    in
    let worker_shutdown_on =
      Spawn_shutdown_on.to_init_worker_state_rpc_query_arg shutdown_on ~connection_timeout
      |> unstage
    in
    let spawn_timeout =
      Timeout.after
        (Option.value spawn_timeout ~default:(default_spawn_timeout ~connection_timeout))
    in
    let spawn_worker () =
      match%bind
        spawn_process
          ~how
          ~env
          ~cd
          ~name
          ~timeout:spawn_timeout
          ~daemonize_args
          ~configured_connection_timeout:connection_timeout
      with
      | Error e -> return (Error e)
      | Ok (id, process) ->
        let id_or_name = Option.value ~default:(Worker_id.to_string id) name in
        (match%bind
           wait_for_daemonization_and_collect_stderr
             id_or_name
             process
             ~timeout:spawn_timeout
         with
         | Error e -> return (Error e)
         | Ok () ->
           wait_for_connection_and_initialize
             ~name
             ~timeout:(Timeout.earlier spawn_timeout (Timeout.after connection_timeout))
             ~on_failure
             ~id
             ~worker_shutdown_on
             worker_state_init_arg)
    in
    let open Spawn_shutdown_on in
    match shutdown_on with
    | Heartbeater_connection_timeout -> spawn_worker ()
    | Connection_closed ->
      fun ~connection_state_init_arg ->
        spawn_worker ()
        >>=? fun worker ->
        (* If [Connection_state.init] raises, [client_internal] will close the Rpc
           connection, causing the worker to shutdown. *)
        Connection.client_with_worker_shutdown_on_disconnect
          worker
          connection_state_init_arg
    | Called_shutdown_function -> spawn_worker ()
  ;;

  module Spawn_exn_shutdown_on = Shutdown_on (Monad.Ident)

  let spawn_exn
    (type a)
    ?how
    ?name
    ?env
    ?connection_timeout
    ?spawn_timeout
    ?cd
    ~on_failure
    ?umask
    ~(shutdown_on : a Spawn_exn_shutdown_on.t)
    ~redirect_stdout
    ~redirect_stderr
    init_arg
    : a
    =
    let open Spawn_exn_shutdown_on in
    match shutdown_on with
    | Connection_closed ->
      fun ~connection_state_init_arg ->
        spawn
          ?how
          ?name
          ?env
          ?connection_timeout
          ?spawn_timeout
          ?cd
          ~on_failure
          ?umask
          ~shutdown_on:Connection_closed
          ~redirect_stdout
          ~redirect_stderr
          init_arg
          ~connection_state_init_arg
        >>| ok_exn
    | Heartbeater_connection_timeout ->
      spawn
        ?how
        ?name
        ?env
        ?connection_timeout
        ?spawn_timeout
        ?cd
        ~on_failure
        ?umask
        ~shutdown_on:Heartbeater_connection_timeout
        ~redirect_stdout
        ~redirect_stderr
        init_arg
      >>| ok_exn
    | Called_shutdown_function ->
      spawn
        ?how
        ?name
        ?env
        ?connection_timeout
        ?spawn_timeout
        ?cd
        ~on_failure
        ?umask
        ~shutdown_on:Called_shutdown_function
        ~redirect_stdout
        ~redirect_stderr
        init_arg
      >>| ok_exn
  ;;

  module Deprecated = struct
    let spawn_and_connect
      ?how
      ?name
      ?env
      ?connection_timeout
      ?spawn_timeout
      ?cd
      ~on_failure
      ?umask
      ~redirect_stdout
      ~redirect_stderr
      ~connection_state_init_arg
      worker_state_init_arg
      =
      spawn
        ?how
        ?name
        ?env
        ?connection_timeout
        ?spawn_timeout
        ?cd
        ?umask
        ~redirect_stdout
        ~redirect_stderr
        ~on_failure
        ~shutdown_on:Heartbeater_connection_timeout
        worker_state_init_arg
      >>=? fun worker ->
      with_shutdown_on_error worker ~f:(fun () ->
        Connection.client worker connection_state_init_arg)
      >>| Or_error.map ~f:(fun conn -> worker, conn)
    ;;

    let spawn_and_connect_exn
      ?how
      ?name
      ?env
      ?connection_timeout
      ?spawn_timeout
      ?cd
      ~on_failure
      ?umask
      ~redirect_stdout
      ~redirect_stderr
      ~connection_state_init_arg
      worker_state_init_arg
      =
      spawn_and_connect
        ?how
        ?name
        ?env
        ?connection_timeout
        ?spawn_timeout
        ?cd
        ~on_failure
        ?umask
        ~redirect_stdout
        ~redirect_stderr
        ~connection_state_init_arg
        worker_state_init_arg
      >>| ok_exn
    ;;
  end

  module For_internal_testing = struct
    module Spawn_in_foreground_result = Spawn_in_foreground_aux_result

    let spawn_in_foreground = spawn_in_foreground_aux
    let master_app_rpc_settings () = (get_master_state_exn ()).app_rpc_settings
  end

  let init_worker_state_impl =
    Rpc.Rpc.implement
      Init_worker_state_rpc.rpc
      (fun
          ( rpc_connection
          , (_ :
              ( Function_creator.worker_state
                , Function_creator.connection_state )
                Utils.Internal_connection_state.t1
                Set_once.t) )
          { Init_worker_state_rpc.worker_shutdown_on; worker; arg }
        ->
         let init_finished =
           Utils.try_within ~monitor (fun () ->
             let%bind on_connection_closed =
               match worker_shutdown_on with
               | Heartbeater_connection_timeout heartbeater_master ->
                 let%map `Connected =
                   Heartbeater_master.connect_and_shutdown_on_disconnect_exn
                     heartbeater_master
                 in
                 `Keep_going
               | Connection_closed { connection_timeout = _ } -> return `Shutdown
               | Called_shutdown_function -> return `Keep_going
             in
             let init_finished = User_functions.init_worker_state arg in
             (match on_connection_closed with
              | `Keep_going -> ()
              | `Shutdown ->
                (* The connection that is used to dispatch this rpc is a short-lived
                   connection that is only used to dispatch this rpc. Under normal
                   operation, this connection will be closed by the master after receiving
                   an RPC response. If the connection closes before we finished running
                   [init_worker_state] then we can force ourselves to shutdown early. This
                   is purely a "shutdown early" bit of code. If we remove this code, the
                   worker will still shutdown eventually, but it will be after worker
                   initialization and after [connection_timeout] when the worker realizes
                   that it hasn't received an initial connection from the master. *)
                Rpc.Connection.close_finished rpc_connection
                >>> fun () ->
                if not (Deferred.is_determined init_finished)
                then (
                  [%log.error
                    "Rpc_parallel: worker connection closed during init... Shutting down"];
                  Shutdown.shutdown 254));
             init_finished)
         in
         Set_once.set_exn
           (get_worker_state_exn ()).initialized
           (`Init_started
             (init_finished >>|? fun (_ : Function_creator.worker_state) -> `Initialized));
         match%map init_finished with
         | Error e -> Error.raise e
         | Ok state ->
           (match worker_shutdown_on with
            | Heartbeater_connection_timeout (_ : Heartbeater_master.t)
            | Called_shutdown_function -> ()
            | Connection_closed { connection_timeout } ->
              Clock.after connection_timeout
              >>> fun () ->
              if not worker_state.client_has_connected
              then (
                [%log.error_string
                  "Rpc_parallel: worker timed out waiting for client connection... \
                   Shutting down"];
                Shutdown.shutdown 254));
           Hashtbl.add_exn worker_state.states ~key:worker ~data:state)
  ;;

  let init_connection_state_impl =
    Rpc.Rpc.implement
      Init_connection_state_rpc.rpc
      (fun
          (connection, internal_conn_state)
          { worker_id; worker_shutdown_on_disconnect; arg = init_arg }
        ->
         worker_state.client_has_connected <- true;
         let worker_state = Hashtbl.find_exn worker_state.states worker_id in
         if worker_shutdown_on_disconnect
         then (
           Rpc.Connection.close_reason ~on_close:`finished connection
           >>> fun reason ->
           [%log.info
             "Rpc_parallel: initial client connection closed... Shutting down."
               (reason : Info.t)];
           Shutdown.shutdown 0);
         let%map conn_state =
           Utils.try_within_exn ~monitor (fun () ->
             User_functions.init_connection_state ~connection ~worker_state init_arg)
         in
         Set_once.set_exn
           internal_conn_state
           { Utils.Internal_connection_state.worker_id; conn_state; worker_state })
  ;;

  let shutdown_impl =
    Rpc.One_way.implement
      Shutdown_rpc.rpc
      (fun _conn_state () ->
        [%log.info_string "Rpc_parallel: Got shutdown rpc... Shutting down."];
        Shutdown.shutdown 0)
      ~on_exception:Close_connection
  ;;

  let close_server_impl =
    Rpc.One_way.implement
      Close_server_rpc.rpc
      (fun (_conn, conn_state) () ->
        let { Utils.Internal_connection_state.worker_id; _ } =
          Set_once.get_exn conn_state
        in
        let global_state = get_worker_state_exn () in
        match Hashtbl.find global_state.my_worker_servers worker_id with
        | None -> ()
        | Some server ->
          Tcp.Server.close server.server
          >>> fun () ->
          Hashtbl.remove global_state.my_worker_servers worker_id;
          Hashtbl.remove worker_state.states worker_id)
      ~on_exception:Close_connection
  ;;

  let async_log_impl =
    Rpc.Pipe_rpc.implement
      Async_log_rpc.rpc
      (fun _conn_state () ->
        let r, w = Pipe.create () in
        let new_output =
          Log.Output.create
            ~flush:(fun () -> Deferred.ignore_m (Pipe.downstream_flushed w))
            (fun msgs ->
              if not (Pipe.is_closed w)
              then Queue.iter msgs ~f:(fun msg -> Pipe.write_without_pushback w msg);
              return ())
        in
        Log.Global.set_output (new_output :: Log.Global.get_output ());
        (* Remove this new output upon the pipe closing. *)
        upon (Pipe.closed w) (fun () ->
          let new_outputs =
            List.filter (Log.Global.get_output ()) ~f:(fun output ->
              not (phys_equal output new_output))
          in
          Log.Global.set_output new_outputs);
        return (Ok r))
      ~leave_open_on_exception:true
  ;;

  let worker_server_rpc_settings_impl =
    Rpc.Rpc.implement Worker_server_rpc_settings_rpc.rpc (fun (_conn, conn_state) () ->
      let { Utils.Internal_connection_state.worker_id; _ } =
        Set_once.get_exn conn_state
      in
      let global_state = get_worker_state_exn () in
      match Hashtbl.find global_state.my_worker_servers worker_id with
      | None ->
        raise_s
          [%message "Failed to find state associated with worker" (worker_id : Id.t)]
      | Some server -> return server.rpc_settings)
  ;;

  let () =
    worker_state.implementations
    <- [ init_worker_state_impl
       ; init_connection_state_impl
       ; shutdown_impl
       ; close_server_impl
       ; async_log_impl
       ; worker_server_rpc_settings_impl
       ]
       @ worker_state.implementations;
    Hashtbl.add_exn
      worker_implementations
      ~key:worker_state.type_
      ~data:(Worker_implementations.T worker_state.implementations)
  ;;
end

(* Start an Rpc server based on the implementations defined in the [Make] functor for this
   worker type. Return a [Host_and_port.t] describing the server *)
let worker_main backend_settings ~worker_env =
  let { Worker_env.config; maybe_release_daemon } = worker_env in
  let rpc_settings = Worker_config.app_rpc_settings config in
  let id = Worker_config.worker_id config in
  let register my_host_and_port =
    match%map
      rpc_connection_with_client
        backend_settings
        ~rpc_settings
        (Tcp.Where_to_connect.of_host_and_port config.master)
        (fun conn -> Rpc.Rpc.dispatch Register_rpc.rpc conn (id, my_host_and_port))
    with
    | Error error -> failwiths "Worker failed to register" error [%sexp_of: Error.t]
    | Ok (Error e) -> failwiths "Worker failed to register" e [%sexp_of: Error.t]
    | Ok (Ok `Shutdown) -> failwith "Got [`Shutdown] on register"
    | Ok (Ok `Registered) -> ()
  in
  (* We want the following two things to occur:

     (1) Catch exceptions in workers and report them back to the master
     (2) Write the exceptions to stderr *)
  let setup_exception_handling () =
    Scheduler.within (fun () ->
      Monitor.detach_and_get_next_error Monitor.main
      >>> fun exn ->
      (* We must be careful that this code here doesn't raise *)
      rpc_connection_with_client
        backend_settings
        ~rpc_settings
        (Tcp.Where_to_connect.of_host_and_port config.master)
        (fun conn ->
           Rpc.Rpc.dispatch
             Handle_exn_rpc.rpc
             conn
             { id; name = config.name; error = Error.of_exn exn })
      >>> fun _ ->
      [%log.error_format !"Rpc_parallel: %{Exn} ... Shutting down." exn];
      Shutdown.shutdown 254)
  in
  (* Ensure we do not leak processes. Make sure we have initialized successfully, meaning
     we have heartbeats with the master established if the user wants them. *)
  let setup_cleanup_on_timeout () =
    Clock.after config.connection_timeout
    >>> fun () ->
    match Set_once.get (get_worker_state_exn ()).initialized with
    | None ->
      [%log.error_string
        "Rpc_parallel: Timeout getting Init_worker_state rpc from master... Shutting \
         down."];
      Shutdown.shutdown 254
    | Some (`Init_started initialize_result) ->
      initialize_result
      >>> (function
       | Ok `Initialized -> ()
       | Error e -> Error.raise e)
  in
  match Hashtbl.find worker_implementations config.worker_type with
  | None ->
    failwith
      "Worker could not find RPC implementations. Make sure the Parallel.Make () functor \
       is applied in the worker. It is suggested to make this toplevel."
  | Some (Worker_implementations.T worker_implementations) ->
    start_server
      backend_settings
      ~implementations:worker_implementations
      ~initial_connection_state:(fun _address connection ->
        connection, Set_once.create ())
      ~rpc_settings
      ~where_to_listen:Tcp.Where_to_listen.of_port_chosen_by_os
    >>> fun server ->
    let host = Unix.gethostname () in
    let port = Tcp.Server.listening_on server.server in
    let global_state = get_worker_state_exn () in
    Hashtbl.add_exn global_state.my_worker_servers ~key:id ~data:server;
    register (Host_and_port.create ~host ~port)
    >>> fun () ->
    setup_exception_handling ();
    setup_cleanup_on_timeout ();
    (* Daemonize as late as possible but still before running any user code. This lets us
       read any setup errors from stderr *)
    maybe_release_daemon ()
;;

module Expert = struct
  module Worker_env = Worker_env

  let worker_init_before_async_exn () =
    match Utils.whoami () with
    | `Master ->
      failwith
        "[worker_init_before_async_exn] should not be called in a process that was not \
         spawned."
    | `Worker ->
      if Scheduler.is_running ()
      then
        failwith
          "[worker_init_before_async_exn] must be called before the async scheduler has \
           been started.";
      Utils.clear_env ();
      let config =
        try Sexp.input_sexp In_channel.stdin |> Worker_config.t_of_sexp with
        | exn ->
          raise_s
            [%message
              "Unable to read worker config from stdin. Make sure nothing is read from \
               stdin before [worker_init_before_async_exn] is called."
                (exn : Exn.t)]
      in
      let maybe_release_daemon =
        match config.daemonize_args with
        | `Don't_daemonize ->
          Core_unix.chdir config.cd;
          Fn.id
        | `Daemonize { Daemonize_args.umask; redirect_stderr; redirect_stdout } ->
          (* The worker is started via SSH. We want to go to the background so we can
             close the SSH connection, but not until we've connected back to the master
             via Rpc. This allows us to report any initialization errors to the master via
             the SSH connection. *)
          let redirect_stdout = Utils.to_daemon_fd_redirection redirect_stdout in
          let redirect_stderr = Utils.to_daemon_fd_redirection redirect_stderr in
          Staged.unstage
            (Daemon.daemonize_wait
               ~cd:config.cd
               ~redirect_stdout
               ~redirect_stderr
               ?umask
               ())
      in
      { Worker_env.config; maybe_release_daemon }
  ;;

  let start_worker_server_exn backend worker_env =
    let rpc_settings = Worker_env.config worker_env |> Worker_config.app_rpc_settings in
    let backend_settings =
      Worker_env.config worker_env |> Worker_config.backend_settings
    in
    let worker_command_args =
      Worker_env.config worker_env |> Worker_config.worker_command_args
    in
    let backend_and_settings = Backend.Settings.with_backend backend_settings backend in
    init_master_state backend_and_settings ~rpc_settings ~worker_command_args;
    worker_main backend_settings ~worker_env
  ;;

  let start_master_server_exn
    ?rpc_max_message_size
    ?rpc_buffer_age_limit
    ?rpc_handshake_timeout
    ?rpc_heartbeat_config
    ?(pass_name = true)
    backend_and_settings
    ~worker_command_args
    ()
    =
    match Utils.whoami () with
    | `Worker -> failwith "Do not call [init_master_exn] in a spawned worker"
    | `Master ->
      let rpc_settings =
        Rpc_settings.create_with_env_override
          ~max_message_size:rpc_max_message_size
          ~buffer_age_limit:rpc_buffer_age_limit
          ~handshake_timeout:rpc_handshake_timeout
          ~heartbeat_config:rpc_heartbeat_config
      in
      init_master_state
        backend_and_settings
        ~rpc_settings
        ~worker_command_args:(User_supplied { args = worker_command_args; pass_name })
  ;;

  let worker_command backend_and_settings =
    let open Command.Let_syntax in
    Command.async
      ~summary:"internal use only"
      (let%map_open _name = anon (maybe ("NAME" %: string)) in
       let worker_env = worker_init_before_async_exn () in
       fun () ->
         start_worker_server_exn backend_and_settings worker_env;
         Deferred.never ())
      ~behave_nicely_in_pipeline:false
  ;;
end

module State = struct
  type t = [ `started ]

  let get () = Option.map (Set_once.get global_state) ~f:(fun _ -> `started)
end

module For_testing = struct
  let initialize backend_and_settings here =
    if Core.am_running_test
    then (
      match Utils.whoami () with
      | `Master ->
        For_testing_internal.set_initialize_source_code_position here;
        (match State.get () with
         | Some `started ->
           Backend.assert_already_initialized_with_same_backend
             (Backend_and_settings.backend backend_and_settings)
         | None ->
           init_master_state
             backend_and_settings
             ~rpc_settings:Rpc_settings.default
             ~worker_command_args:Add_master_pid)
      | `Worker ->
        if For_testing_internal.worker_should_initialize here
        then (
          let env = Expert.worker_init_before_async_exn () in
          Expert.start_worker_server_exn
            (Backend_and_settings.backend backend_and_settings)
            env;
          never_returns (Scheduler.go ())))
  ;;
end

let start_app
  ?rpc_max_message_size
  ?rpc_buffer_age_limit
  ?rpc_handshake_timeout
  ?rpc_heartbeat_config
  ?when_parsing_succeeds
  ?complete_subcommands
  ?add_validate_parsing_flag
  ?argv
  backend_and_settings
  command
  =
  match Utils.whoami () with
  | `Worker ->
    let worker_env = Expert.worker_init_before_async_exn () in
    Expert.start_worker_server_exn
      (Backend_and_settings.backend backend_and_settings)
      worker_env;
    never_returns (Scheduler.go ())
  | `Master ->
    let rpc_settings =
      Rpc_settings.create_with_env_override
        ~max_message_size:rpc_max_message_size
        ~buffer_age_limit:rpc_buffer_age_limit
        ~handshake_timeout:rpc_handshake_timeout
        ~heartbeat_config:rpc_heartbeat_config
    in
    init_master_state
      backend_and_settings
      ~rpc_settings
      ~worker_command_args:Add_master_pid;
    Command_unix.run
      ?add_validate_parsing_flag
      ?when_parsing_succeeds
      ?complete_subcommands
      ?argv
      command
;;
