open Core.Std
open Async.Std

module Worker_type_id = Unique_id.Int ()
module Worker_id      = Unique_id.Int ()

(* Applications of the [Make_worker()] functor have the side effect of populating
   [implementations] with all the implementations needed to act as that kind of worker.

   It *must* be the case that [implementations] gets populated in the spawned worker,
   which is why it is highly recommended to put all the [Make_worker()] calls top-level *)
let implementations = Worker_type_id.Table.create ()
let add_implementation worker_type impl =
  Hashtbl.add_multi implementations ~key:worker_type ~data:impl


type static_state =
  {
    (* The [Host_and_port.t] corresponding to one's own Rpc server. See comment in
       [start_app] for why it is an [Ivar.t] *)
    my_server: Host_and_port.t Ivar.t;
    (* Used to facilitate timeout of connecting to a new worker *)
    pending: (Rpc.Connection.t * Host_and_port.t) Or_error.t Ivar.t Worker_id.Table.t;
    (* Connected workers *)
    workers: Rpc.Connection.t Host_and_port.Table.t;
    (* Callbacks for worker exceptions and disconnects *)
    on_failures: (Error.t -> unit) Worker_id.Table.t;
    (* Arguments used when spawning a new worker *)
    worker_command_args : string list
  }

(* Each running instance has the capability to work as a master. This state includes
   the connections to all its workers and its own server information (needed to spawn
   workers) *)
let the_state : static_state Set_once.t = Set_once.create ()

let get_state_exn () =
  match Set_once.get the_state with
  | None -> failwith "State should have been set already"
  | Some state -> state

(* Get the location of the currently running binary. This is the best choice we have
   because the executable could have been deleted *)
let our_binary =
  let our_binary_lazy =
    lazy (Unix.getpid () |> Pid.to_int |> sprintf "/proc/%d/exe" |> Unix.readlink)
  in
  fun () -> Lazy.force our_binary_lazy

let our_md5 =
  let our_md5_lazy =
    lazy begin
      our_binary ()
      >>= fun binary ->
      Process.run ~prog:"md5sum" ~args:[binary] ()
      >>|? fun our_md5 ->
      let our_md5, _ = String.lsplit2_exn ~on:' ' our_md5 in
      our_md5
    end
  in fun () -> Lazy.force our_md5_lazy

(* Like [Monitor.try_with], but raise any additional exceptions (raised after [f ()] has
   been determined) to the specified monitor. *)
let try_within ~monitor f =
  let ivar = Ivar.create () in
  Scheduler.within ~monitor (fun () ->
    Monitor.try_with ~extract_exn:true ~run:`Now ~rest:`Raise f
    >>> fun r ->
    Ivar.fill ivar (Result.map_error r ~f:Error.of_exn)
  );
  Ivar.read ivar

(* Any exceptions that are raised before [f ()] is determined will be raised to the
   current monitor. Exceptions raised after [f ()] is determined will be raised to the
   passed in monitor *)
let try_within_exn ~monitor f =
  try_within ~monitor f
  >>| function
  | Ok x -> x
  | Error e -> Error.raise e

module Environment = struct
  let is_child_env_var = "ASYNC_PARALLEL_IS_CHILD_MACHINE"

  let whoami () =
    match Sys.getenv is_child_env_var with
    | Some id_str -> `Worker id_str
    | None -> `Master

  let clear () = Unix.unsetenv is_child_env_var

  let validate env =
    match List.find env ~f:(fun (key, _) -> key = is_child_env_var) with
    | Some e ->
      Or_error.error
        "Environment variable conflicts with Rpc_parallel machinery"
        e [%sexp_of: string*string]
    | None -> Ok ()

  let create_worker ~extra ~id =
    let open Or_error.Monad_infix in
    validate extra
    >>| fun () ->
    extra @ [is_child_env_var, id]
end

module Remote_executable = struct
  type 'a t = { host: string; path: string; host_key_checking: string list }
  [@@deriving fields]

  let hostkey_checking_options opt  =
    match opt with
    | None      -> [       (* Use ssh default *)       ]
    | Some `Ask -> [ "-o"; "StrictHostKeyChecking=ask" ]
    | Some `No  -> [ "-o"; "StrictHostKeyChecking=no"  ]
    | Some `Yes -> [ "-o"; "StrictHostKeyChecking=yes" ]

  let existing_on_host ~executable_path ?strict_host_key_checking host =
    { host;
      path = executable_path;
      host_key_checking = hostkey_checking_options strict_host_key_checking }

  let copy_to_host ~executable_dir ?strict_host_key_checking host =
    our_binary ()
    >>= fun binary ->
    let our_basename = Filename.basename binary in
    Process.run ~prog:"mktemp"
      ~args:["-u"; sprintf "%s.XXXXXXXX" our_basename] ()
    >>=? fun new_basename ->
    let options = hostkey_checking_options strict_host_key_checking in
    let path = String.strip (executable_dir ^/ new_basename) in
    Process.run ~prog:"scp"
      ~args:(options @ [binary; sprintf "%s:%s" host path]) ()
    >>|? Fn.const { host; path; host_key_checking=options }

  let delete executable =
    Process.run ~prog:"ssh"
      ~args:(executable.host_key_checking @ [executable.host; "rm"; executable.path]) ()
    >>|? Fn.const ()

  let command env binary =
    let cheesy_escape str = Sexp.to_string (String.sexp_of_t str) in
    let env =
      String.concat
        (List.map env ~f:(fun (key, data) -> key ^ "=" ^ cheesy_escape data))
        ~sep:" "
    in
    sprintf "%s %s" env binary

  let run exec ~env ~worker_command_args =
    our_md5 ()
    >>=? fun md5 ->
    Process.run ~prog:"ssh"
      ~args:(exec.host_key_checking @ [exec.host; "md5sum"; exec.path]) ()
    >>=? fun remote_md5 ->
    let remote_md5, _ = String.lsplit2_exn ~on:' ' remote_md5 in
    if md5 <> remote_md5 then
      Deferred.Or_error.errorf
        "The remote executable %s:%s does not match the local executable"
        exec.host exec.path
    else
      Process.create ~prog:"ssh"
        ~args:(exec.host_key_checking @
               [exec.host; command env exec.path] @
               worker_command_args) ()
end

let start_server ?max_message_size ?handshake_timeout
      ?heartbeat_config ?(where_to_listen=Tcp.on_port_chosen_by_os) implementations =
  let state = get_state_exn () in
  let implementations =
    Rpc.Implementations.create_exn ~implementations
      ~on_unknown_rpc:`Close_connection
  in
  Rpc.Connection.serve ~implementations ~initial_connection_state:(fun _ _ -> ())
    ?max_message_size ?handshake_timeout
    ?heartbeat_config ~where_to_listen ()
  >>| fun serv ->
  let host_and_port =
    Host_and_port.create ~host:(Unix.gethostname ())
      ~port: (Tcp.Server.listening_on serv)
  in
  Ivar.fill state.my_server host_and_port;
  host_and_port

module Fd_redirection : sig
  type t = [
    | `Dev_null
    | `File_append of string
    | `File_truncate of string
  ] [@@deriving sexp]

  val to_daemon_fd_redirection
    :  t
    -> Daemon.Fd_redirection.t
end = struct
  type t = [
    | `Dev_null
    | `File_append of string
    | `File_truncate of string
  ] [@@deriving sexp]

  let to_daemon_fd_redirection = function
    | `Dev_null -> `Dev_null
    | `File_append s -> `File_append s
    | `File_truncate s -> `File_truncate s
end

module Function = struct
  module Id = Unique_id.Int ()

  module Function_piped = struct
    type ('worker, 'query, 'response) t = ('query, 'response, Error.t) Rpc.Pipe_rpc.t

    let make_impl ~monitor ~f protocol =
      Rpc.Pipe_rpc.implement protocol
        (fun () arg ~aborted:_ -> try_within ~monitor (fun () -> f arg))

    let make_proto ?name ~bin_input ~bin_output () =
      let name = match name with
        | None -> sprintf "rpc_parallel_piped_%s" (Id.to_string (Id.create ()))
        | Some n -> n
      in
      Rpc.Pipe_rpc.create
        ~name
        ~version:0
        ~bin_query:bin_input
        ~bin_response:bin_output
        ~bin_error:Error.bin_t
        ()
  end

  module Function_plain = struct
    type ('worker, 'query, 'response) t = ('query, 'response) Rpc.Rpc.t

    let make_impl ~monitor ~f protocol =
      Rpc.Rpc.implement protocol (fun () arg ->
        (* We want to raise any exceptions from [f arg] to the current monitor (handled
           by Rpc) so the caller can see it. Additional exceptions will be handled by the
           specified monitor *)
        try_within_exn ~monitor (fun () -> f arg))

    let make_proto ?name ~bin_input ~bin_output () =
      let name = match name with
        | None -> sprintf "rpc_parallel_plain_%s" (Id.to_string (Id.create ()))
        | Some n -> n
      in
      Rpc.Rpc.create
        ~name
        ~version:0
        ~bin_query:bin_input
        ~bin_response:bin_output
  end

  type ('worker, 'query, 'response) t =
    | Plain of ('worker, 'query, 'response) Function_plain.t
    | Piped
      :  ('worker, 'query, 'response) Function_piped.t
         *  ('r, 'response Pipe.Reader.t) Type_equal.t
      -> ('worker, 'query, 'r) t

  let create_rpc ~worker_type ~monitor ?name ~f ~bin_input ~bin_output () =
    let proto = Function_plain.make_proto ?name ~bin_input ~bin_output () in
    add_implementation worker_type (Function_plain.make_impl ~monitor ~f proto);
    Plain proto

  let create_pipe ~worker_type ~monitor ?name ~f ~bin_input ~bin_output () =
    let proto = Function_piped.make_proto ?name ~bin_input ~bin_output () in
    add_implementation worker_type (Function_piped.make_impl ~monitor ~f proto);
    Piped (proto, Type_equal.T)

  let of_async_rpc ~worker_type ~monitor ~f proto =
    add_implementation worker_type (Function_plain.make_impl ~monitor ~f proto);
    Plain proto

  let of_async_pipe_rpc ~worker_type ~monitor ~f proto =
    add_implementation worker_type (Function_piped.make_impl ~monitor ~f proto);
    Piped (proto, Type_equal.T)

  let run (type response) (t : (_, _, response) t) connection ~arg
    : response Or_error.t Deferred.t =
    match t with
    | Plain proto -> Rpc.Rpc.dispatch proto connection arg
    | Piped (proto, Type_equal.T) ->
      Rpc.Pipe_rpc.dispatch proto connection arg
      >>| fun result ->
      Or_error.join result
      |> Or_error.map ~f:(fun (reader, _) -> reader)
end

module type Functions = sig
  type worker
  type 'worker functions
  val functions : worker functions
end

module type Worker = sig
  type t [@@deriving bin_io]
  type 'a functions
  type init_arg

  val functions : t functions

  val spawn
    :  ?where : [`Local | `Remote of _ Remote_executable.t]
    -> ?disown : bool
    -> ?env : (string * string) list
    -> ?rpc_max_message_size  : int
    -> ?rpc_handshake_timeout : Time.Span.t
    -> ?rpc_heartbeat_config : Rpc.Connection.Heartbeat_config.t
    -> ?connection_timeout:Time.Span.t
    -> ?cd : string
    -> ?umask : int
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
    -> ?cd : string
    -> ?umask : int
    -> redirect_stdout : Fd_redirection.t
    -> redirect_stderr : Fd_redirection.t
    -> init_arg
    -> on_failure : (Error.t -> unit)
    -> t Deferred.t

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

  val async_log : t -> Log.Message.Stable.V2.t Pipe.Reader.t Deferred.Or_error.t

  val kill     : t -> unit Or_error.t Deferred.t
  val kill_exn : t -> unit            Deferred.t

  val host_and_port : t -> Host_and_port.t
end

module type Creator = sig
  type worker [@@deriving bin_io]
  type state

  val create_rpc
    :  ?name: string
    -> f:(state -> 'query -> 'response Deferred.t)
    -> bin_input: 'query Bin_prot.Type_class.t
    -> bin_output: 'response Bin_prot.Type_class.t
    -> unit
    -> (worker, 'query, 'response) Function.t

  val create_pipe :
    ?name: string
    -> f:(state -> 'query -> 'response Pipe.Reader.t Deferred.t)
    -> bin_input: 'query Bin_prot.Type_class.t
    -> bin_output: 'response Bin_prot.Type_class.t
    -> unit
    -> (worker, 'query, 'response Pipe.Reader.t) Function.t

  val of_async_rpc
    :  f:(state -> 'query -> 'response Deferred.t)
    -> ('query, 'response) Rpc.Rpc.t
    -> (worker, 'query, 'response) Function.t

  val of_async_pipe_rpc
    :  f:(state -> 'query -> 'response Pipe.Reader.t Deferred.t)
    -> ('query, 'response, Error.t) Rpc.Pipe_rpc.t
    -> (worker, 'query, 'response Pipe.Reader.t) Function.t

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

module type Worker_spec = sig
  type 'worker functions

  type init_arg [@@deriving bin_io]
  type state
  val init : init_arg -> state Deferred.t

  module Functions(C:Creator with type state := state) : Functions
    with type 'a functions := 'a functions
    with type worker := C.worker
end

module Worker_config = struct
  type t =
    { worker_type           : Worker_type_id.t
    ; disown                : bool
    ; master                : Host_and_port.t
    ; rpc_max_message_size  : int option
    ; rpc_handshake_timeout : Time.Span.t option
    ; rpc_heartbeat_config  : Rpc.Connection.Heartbeat_config.t option
    ; cd                    : string option
    ; umask                 : int option
    ; redirect_stdout       : Fd_redirection.t
    ; redirect_stderr       : Fd_redirection.t
    } [@@deriving sexp]
end

(* Rpc implemented by workers *)
module Shutdown_rpc = struct
  let rpc =
    Rpc.One_way.create
      ~name:"shutdown_rpc"
      ~version:0
      ~bin_msg:Unit.bin_t

  let implementation = Rpc.One_way.implement rpc (fun () () ->
    eprintf "Got kill command. Shutting down.\n";
    Shutdown.shutdown 0)

  let dispatch connection =
    match Rpc.One_way.dispatch rpc connection () with
    | Error _ as result -> return result
    | Ok () -> Rpc.Connection.close_finished connection >>| fun () -> Ok ()
end

module Log_rpc = struct
  let rpc =
    Rpc.Pipe_rpc.create
      ~name:"log_rpc"
      ~version:0
      ~bin_query:Unit.bin_t
      ~bin_response:Log.Message.Stable.V2.bin_t
      ~bin_error:Error.bin_t
      ()

  let implementation =
    Rpc.Pipe_rpc.implement rpc
      (fun () () ~aborted:_ ->
         let r, w = Pipe.create () in
         let new_output = Log.Output.create (fun msgs -> Pipe.transfer_in w ~from:msgs) in
         Log.Global.set_output (new_output::Log.Global.get_output ());
         return (Ok r))
end

(* All workers must also implement [Init_rpc.rpc], but this lives inside the
   [Make_worker()] functor because it is specific to each worker *)
let worker_implementations = [Shutdown_rpc.implementation; Log_rpc.implementation]

(* Rpcs implemented by master *)
module Register_rpc = struct
  type t = Worker_id.t * Host_and_port.t [@@deriving bin_io]

  let rpc =
    Rpc.Rpc.create
      ~name:"register_worker_rpc"
      ~version:0
      ~bin_query:bin_t
      ~bin_response:Unit.bin_t

  let implementation ?max_message_size ?handshake_timeout ?heartbeat_config () =
    Rpc.Rpc.implement rpc (fun () (id, worker_hp) ->
      let state = get_state_exn () in
      Rpc.Connection.client
        ?max_message_size
        ?handshake_timeout
        ?heartbeat_config
        ~host:(Host_and_port.host worker_hp)
        ~port:(Host_and_port.port worker_hp) ()
      >>= function
      | Error e ->
        begin
          (* Failed to connect to worker, it must be dead *)
          match Hashtbl.find state.pending id with
          | None ->
            (* We already returned a failure to the [spawn_worker] caller *)
            return ()
          | Some ivar ->
            return (Ivar.fill ivar (Error (Error.of_exn e)))
        end
      | Ok connection ->
        match Hashtbl.find state.pending id with
        | None ->
          (* We already returned a failure to the [spawn_worker] caller,
             tell the worker to shutdown *)
          Shutdown_rpc.dispatch connection >>| ignore
        | Some ivar ->
          (* We are connected and haven't timed out *)
          Ivar.fill ivar (Ok (connection, worker_hp)) |> return)
end

module Handle_exn_rpc = struct
  type t = Worker_id.t * Error.t [@@deriving bin_io]

  let rpc =
    Rpc.Rpc.create
      ~name:"handle_worker_exn_rpc"
      ~version:0
      ~bin_query:bin_t
      ~bin_response:Unit.bin_t

  let implementation =
    Rpc.Rpc.implement rpc (fun () (id, error) ->
      let state = get_state_exn () in
      let on_failure = Hashtbl.find_exn state.on_failures id in
      on_failure error;
      return ())
end

let master_implementations ?max_message_size ?handshake_timeout ?heartbeat_config () =
  [Register_rpc.implementation ?max_message_size ?handshake_timeout ?heartbeat_config ();
   Handle_exn_rpc.implementation]

let init_master_state ~worker_command_args =
  match Set_once.get the_state with
  | Some _state -> failwith "Master state should not have been set up twice"
  | None ->
    let pending = Worker_id.Table.create () in
    let workers = Host_and_port.Table.create () in
    let on_failures = Worker_id.Table.create () in
    let my_server = Ivar.create () in
    Set_once.set_exn the_state
      {my_server; pending; workers; on_failures; worker_command_args}

(* Find the implementations for the given worker type (determined by the [Id.t] given to
   it at the top level of the [Make_worker] functor application), host an Rpc server with
   these implementations and return back a [Host_and_port.t] describing the server *)
let worker_main ~id ~(config : Worker_config.t) ~release_daemon =
  match Hashtbl.find implementations config.worker_type with
  | None ->
    failwith
      "Worker could not find RPC implementations. Make sure the Parallel.Make_worker () \
       functor is applied in the worker. It is suggested to make this toplevel."
  | Some impls ->
    let master_implementations =
      master_implementations
        ?max_message_size:config.rpc_max_message_size
        ?handshake_timeout:config.rpc_handshake_timeout
        ?heartbeat_config:config.rpc_heartbeat_config
        ()
    in
    let implementations = master_implementations @ worker_implementations @ impls in
    start_server
      ?max_message_size:config.rpc_max_message_size
      ?handshake_timeout:config.rpc_handshake_timeout
      ?heartbeat_config:config.rpc_heartbeat_config
      implementations
    >>> fun host_and_port ->
    Rpc.Connection.client
      ?max_message_size:config.rpc_max_message_size
      ?handshake_timeout:config.rpc_handshake_timeout
      ?heartbeat_config:config.rpc_heartbeat_config
      ~host:(Host_and_port.host config.master)
      ~port:(Host_and_port.port config.master) ()
    >>> function
    | Error e -> raise e
    | Ok conn ->
      (* Set up exception handling. We want the following two things to occur:

         1) Catch exceptions in [M.worker_main] and report them back to the master
         2) Write the exceptions to stderr *)
      Scheduler.within (fun () ->
        Monitor.detach_and_get_next_error Monitor.main
        >>> fun exn ->
        Rpc.Rpc.dispatch Handle_exn_rpc.rpc conn (id, Error.of_exn exn)
        >>> fun _ ->
        eprintf !"%{sexp:Exn.t}\n" exn;
        eprintf "Shutting down\n";
        Shutdown.shutdown 254);
      (* Set up cleanup so the worker shuts down when it loses connection to the master
         (unless [config.disown = true]) *)
      (if not config.disown then
         Rpc.Connection.close_finished conn
         >>> fun () ->
         Shutdown.shutdown 254);
      (* Once we've connected back to the master, it's safe to detach. Calling
         [release_daemon] will fail if stdout/stderr redirection fails. This will write
         the uncaught exception to the workers stderr which is read by the master. We
         must not call [release_daemon] after the dispatch because the worker will die at
         a particularly bad time (the master may report the closed connection before
         reading the worker's stderr / RPC handshake or connection closed exceptions
         may report first) *)
      release_daemon ();
      (* Register itself as a worker *)
      ignore (Rpc.Rpc.dispatch_exn Register_rpc.rpc conn (id, host_and_port))

module Make_worker(S:Worker_spec) = struct
  type t = Host_and_port.t [@@deriving bin_io]

  (* A unique identifier for each application of the [Make_worker] functor. Because we are
     running the same executable, the master and the workers will agree on these ids *)
  let worker_type = Worker_type_id.create ()

  (* State associated with this worker *)
  let worker_state : S.state Set_once.t = Set_once.create ()

  (* Schedule everything in [Monitor.main] so no exceptions are lost. Async log
     automatically throws its exceptions to [Monitor.main] so we can't make our own local
     monitor. We detach [Monitor.main] and send exceptions back to the master. *)
  let monitor = Monitor.main

  module Init_rpc = struct
    let rpc =
      Rpc.Rpc.create
        ~name:"worker_init_rpc"
        ~version:0
        ~bin_query:S.bin_init_arg
        ~bin_response:Unit.bin_t
  end

  let () =
    let implementation =
      Rpc.Rpc.implement Init_rpc.rpc (fun () arg ->
        try_within_exn ~monitor (fun () -> S.init arg)
        >>| Set_once.set_exn worker_state)
    in
    add_implementation worker_type implementation

  (* The workers fork. This causes the standard file descriptors to remain open once the
     process has exited. We close them here to avoid a file descriptor leak. *)
  let cleanup_standard_fds process =
    Process.wait process
    >>> fun (exit_or_signal : Unix.Exit_or_signal.t) ->
    (Reader.contents (Process.stderr process) >>> fun s ->
     Writer.write (Lazy.force Writer.stderr) s;
     don't_wait_for (Reader.close (Process.stderr process)));
    don't_wait_for (Writer.close (Process.stdin  process));
    don't_wait_for (Reader.close (Process.stdout process));
    match exit_or_signal with
    | Ok () -> ()
    | Error _ -> eprintf "Worker process %s\n"
                   (Unix.Exit_or_signal.to_string_hum exit_or_signal)

  let run_executable where ~env ~id ~worker_command_args ~input =
    Environment.create_worker ~extra:env ~id |> return
    >>=? fun env ->
    match where with
    | `Local ->
      our_binary ()
      >>= fun binary ->
      Process.create ~prog:binary ~args:worker_command_args ~env:(`Extend env) ()
      >>|? fun p ->
      (* It is important that we start waiting for the child process here. The
         worker it forks daemonizes, thus this process ends. This will happen before
         the worker process quits or closes his RPC connection. If we only [wait]
         once the RPC connection is closed, we will have zombie processes hanging
         around until then. *)
      cleanup_standard_fds p;
      Writer.write_sexp (Process.stdin p) input
    | `Remote exec ->
      Remote_executable.run exec ~env ~worker_command_args
      >>|? fun p ->
      cleanup_standard_fds p;
      Writer.write_sexp (Process.stdin p) input

    let spawn
        ?(where=`Local) ?(disown=false) ?(env=[])
        ?rpc_max_message_size ?rpc_handshake_timeout ?rpc_heartbeat_config
        ?(connection_timeout=(sec 10.)) ?cd ?umask ~redirect_stdout ~redirect_stderr
        init_arg ~on_failure =
    begin match Set_once.get the_state with
    | None ->
      Deferred.Or_error.error_string
        "You must initialize this process to run as a master before calling \
         [spawn]. Either use a top-level [start_app] call or use the [Expert] module."
    | Some st -> Deferred.Or_error.return st
    end
    >>=? fun st ->
    (* generate a unique identifier for this worker *)
    let id = Worker_id.create () in
    let host =
      match where with
      | `Local -> "local"
      | `Remote exec -> Remote_executable.host exec
    in
    let pending_ivar = Ivar.create () in
    Hashtbl.add_exn st.pending ~key:id ~data:pending_ivar;
    Hashtbl.add_exn st.on_failures ~key:id ~data:on_failure;
    Ivar.read st.my_server
    >>= fun master ->
    let input =
      { Worker_config.
        worker_type
      ; disown
      ; master
      ; rpc_max_message_size
      ; rpc_handshake_timeout
      ; rpc_heartbeat_config
      ; cd
      ; umask
      ; redirect_stdout
      ; redirect_stderr
      } |> Worker_config.sexp_of_t
    in
    run_executable where ~env ~id:(Worker_id.to_string id)
      ~worker_command_args:st.worker_command_args ~input
    >>= function
    | Error _ as err ->
      Hashtbl.remove st.pending id;
      Hashtbl.remove st.on_failures id;
      return err
    | Ok () ->
      (* We have successfully copied over the binary and got it running, now we ensure
         that we got a connection from the worker *)
      Clock.with_timeout connection_timeout (Ivar.read pending_ivar)
      >>= function
      | `Timeout ->
        (* Haven't connected to the worker, cleanup *)
        Hashtbl.remove st.pending id;
        Hashtbl.remove st.on_failures id;
        Deferred.Or_error.errorf "Timed out getting connection from %s process" host
      | `Result connection_or_error ->
        Hashtbl.remove st.pending id;
        match connection_or_error with
        | Ok (connection, worker_hp) ->
          begin
            Rpc.Rpc.dispatch Init_rpc.rpc connection init_arg
            >>| function
            | Error _ as err ->
              Hashtbl.remove st.on_failures id;
              err
            | Ok () ->
              Hashtbl.add_exn st.workers ~key:worker_hp ~data:connection;
              (* Setup cleanup on disconnect *)
              (Rpc.Connection.close_finished connection
               >>> fun () ->
               Rpc.Connection.close_reason connection
               >>> fun info ->
               match Hashtbl.find st.workers worker_hp with
               | None ->
                 (* [kill_worker] was called, don't report closed connection *)
                 ()
               | Some _ ->
                 Hashtbl.remove st.workers worker_hp;
                 let error =
                   Error.createf !"Lost connection with worker: %{sexp:Info.t}" info
                 in
                 on_failure error);
              Ok worker_hp
          end
        | Error _ as err ->
          Hashtbl.remove st.on_failures id;
          return err

  let spawn_exn ?where ?disown ?env
        ?rpc_max_message_size ?rpc_handshake_timeout ?rpc_heartbeat_config
        ?connection_timeout ?cd ?umask ~redirect_stdout ~redirect_stderr
        arg ~on_failure =
    spawn ?where ?disown ?env
      ?rpc_max_message_size ?rpc_handshake_timeout ?rpc_heartbeat_config
      ?connection_timeout ?cd ?umask ~redirect_stdout ~redirect_stderr
      arg ~on_failure
    >>| Or_error.ok_exn


(* Unless [disown] was set to true on [spawn] of this worker, the fact that the
   connection was lost means the worker shut itself down. However, there is also
   the possibility that this [t] was sent to another process and that new process
   is calling [run] on it. In this case, there is no connection because no
   connection was ever made. So, we reconnect here to handle both cases. *)
  let get_connection t =
    let state = get_state_exn () in
    match Hashtbl.find state.workers t with
    | Some conn -> Deferred.Or_error.return conn
    | None ->
        Rpc.Connection.client
          ~host:(Host_and_port.host t)
          ~port:(Host_and_port.port t) ()
        >>= function
        | Error e -> Deferred.Or_error.error "Cannot connect to worker" e Exn.sexp_of_t
        | Ok conn -> Deferred.Or_error.return conn

  let run t ~f ~arg =
    get_connection t
    >>=? fun conn ->
    Function.run f conn ~arg

  let run_exn t ~f ~arg =
    run t ~f ~arg
    >>| Or_error.ok_exn

  let kill t =
    match Set_once.get the_state with
    | None -> Deferred.Or_error.error_string "Rpc_parallel master not initialized"
    | Some st ->
      match Hashtbl.find st.workers t with
      | None ->
        Deferred.Or_error.error_string
          "Worker already dead or was spawned from some other process"
      | Some connection ->
        Hashtbl.remove st.workers t;
        Shutdown_rpc.dispatch connection

  let kill_exn t = kill t >>| Or_error.ok_exn

  let host_and_port t = t

  let async_log t =
    get_connection t
    >>=? fun conn ->
    Rpc.Pipe_rpc.dispatch Log_rpc.rpc conn ()
    >>=? fun result ->
    Or_error.map result ~f:(fun (reader, _) -> reader) |> return

  module Function_creator = struct
    type worker = t [@@deriving bin_io]

    let wrap f x = f (Set_once.get_exn worker_state) x

    let create_rpc ?name ~f ~bin_input ~bin_output () =
      Function.create_rpc ~worker_type ~monitor ?name ~f:(wrap f)
        ~bin_input ~bin_output ()

    let create_pipe ?name ~f ~bin_input ~bin_output () =
      Function.create_pipe ~worker_type ~monitor ?name ~f:(wrap f)
        ~bin_input ~bin_output ()

    let of_async_rpc ~f proto =
      Function.of_async_rpc ~worker_type ~monitor ~f:(wrap f) proto

    let of_async_pipe_rpc ~f proto =
      Function.of_async_pipe_rpc ~worker_type ~monitor ~f:(wrap f) proto

    let run = run
    let run_exn = run_exn
  end

  module User_functions = S.Functions(Function_creator)

  let functions = User_functions.functions
end


module State = struct
  type t = [ `started ]

  let get () = Option.map (Set_once.get the_state) ~f:(fun _ -> `started)
end

module Expert = struct
  let run_as_worker_exn ~worker_command_args =
    match Environment.whoami () with
    | `Master ->
      failwith "Could not find worker environment. Workers must be spawned by masters"
    | `Worker id_str ->
      Environment.clear ();
      init_master_state ~worker_command_args;
      let config = Sexp.input_sexp In_channel.stdin |> Worker_config.t_of_sexp in
      let id = Worker_id.of_string id_str in
      (* The worker is started via SSH. We want to go to the background so we can close
         the SSH connection, but not until we've connected back to the master via
         Rpc. This allows us to report any initialization errors to the master via the SSH
         connection. *)
      let redirect_stdout =
        Fd_redirection.to_daemon_fd_redirection config.redirect_stdout
      in
      let redirect_stderr =
        Fd_redirection.to_daemon_fd_redirection config.redirect_stderr
      in
      let release_daemon =
        Staged.unstage (
          (* This call can fail (e.g. cd to a nonexistent directory). The uncaught
             exception will automatically be written to stderr which is read by the
             master *)
          Daemon.daemonize_wait ~redirect_stdout ~redirect_stderr
            ?cd:config.cd
            ?umask:config.umask
            ()
        )
      in
      worker_main ~id ~config ~release_daemon;
      never_returns (Scheduler.go ())

  let init_master_exn ?rpc_max_message_size ?rpc_handshake_timeout ?rpc_heartbeat_config
        ?where_to_listen ?implementations ~worker_command_args () =
    match Environment.whoami () with
    | `Worker _ -> failwith "Do not call [init_master_exn] in a spawned worker"
    | `Master ->
      init_master_state ~worker_command_args;
      (* Start a server with the master implementations *)
      let master_implementations =
        master_implementations
          ?max_message_size:rpc_max_message_size
          ?handshake_timeout:rpc_handshake_timeout
          ?heartbeat_config:rpc_heartbeat_config
          ()
      in
      let implementations =
        Option.value_map ~default:master_implementations implementations
          ~f:(fun imps -> imps @ master_implementations)
      in
      (* The async scheduler will be started in [Command.run]. We must make sure that
         [start_server] has run to completion before any calls to [spawn]. So we use an
         [Ivar.t] in the global state to synchronize these calls *)
      don't_wait_for(
        start_server ?max_message_size:rpc_max_message_size
          ?handshake_timeout:rpc_handshake_timeout
          ?heartbeat_config:rpc_heartbeat_config
          ?where_to_listen implementations >>| Fn.const ())
end

let start_app ?rpc_max_message_size ?rpc_handshake_timeout
      ?rpc_heartbeat_config ?where_to_listen ?implementations command =
  match Environment.whoami () with
  | `Worker _ ->
    Expert.run_as_worker_exn ~worker_command_args:[]
  | `Master ->
    Expert.init_master_exn ?rpc_max_message_size ?rpc_handshake_timeout
      ?rpc_heartbeat_config ?where_to_listen ?implementations
      ~worker_command_args:[] ();
    Command.run command
;;
