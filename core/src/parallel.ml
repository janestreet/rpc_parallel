open Core
open Async

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

module Worker_config = struct
  type t =
    { disown                : bool
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
;;

(* Any exceptions that are raised before [f ()] is determined will be raised to the
   current monitor. Exceptions raised after [f ()] is determined will be raised to the
   passed in monitor *)
let try_within_exn ~monitor f =
  try_within ~monitor f
  >>| function
  | Ok x -> x
  | Error e -> Error.raise e

let hostkey_checking_options opt  =
  match opt with
  | None      -> [       (* Use ssh default *)       ]
  | Some `Ask -> [ "-o"; "StrictHostKeyChecking=ask" ]
  | Some `No  -> [ "-o"; "StrictHostKeyChecking=no"  ]
  | Some `Yes -> [ "-o"; "StrictHostKeyChecking=yes" ]


(* Use /proc/PID/exe to get the currently running executable.
   - argv[0] might have been deleted (this is quite common with jenga)
   - `cp /proc/PID/exe dst` works as expected while `cp /proc/self/exe dst` does not *)
let our_binary =
  let our_binary_lazy =
    lazy (Unix.getpid () |> Pid.to_int |> sprintf "/proc/%d/exe")
  in
  fun () -> Lazy.force our_binary_lazy

let our_md5 =
  let our_md5_lazy =
    lazy begin
      Process.run ~prog:"md5sum" ~args:[our_binary ()] ()
      >>|? fun our_md5 ->
      let our_md5, _ = String.lsplit2_exn ~on:' ' our_md5 in
      our_md5
    end
  in fun () -> Lazy.force our_md5_lazy

module Remote_executable = struct
  type 'a t = { host: string; path: string; host_key_checking: string list }

  let existing_on_host ~executable_path ?strict_host_key_checking host =
    { host;
      path = executable_path;
      host_key_checking = hostkey_checking_options strict_host_key_checking }

  let copy_to_host ~executable_dir ?strict_host_key_checking host =
    let binary = our_binary () in
    let our_basename = Filename.basename binary in
    Process.run ~prog:"mktemp"
      ~args:["-u"; sprintf "%s.XXXXXXXX" our_basename] ()
    >>= function
    | Error e -> return (Error e)
    | Ok new_basename ->
      let options = hostkey_checking_options strict_host_key_checking in
      let path = String.strip (executable_dir ^/ new_basename) in
      Process.run ~prog:"scp"
        ~args:(options @ [binary; sprintf "%s:%s" host path]) ()
      >>| function
      | Error e -> Error e
      | Ok (_:string) -> Ok { host; path; host_key_checking=options }

  let delete executable =
    Process.run ~prog:"ssh"
      ~args:(executable.host_key_checking @ [executable.host; "rm"; executable.path]) ()
    >>| function
    | Error e -> Error e
    | Ok (_:string) -> Ok ()

  let path e = e.path
  let host e = e.host
end

(* The handshake process to connect to a new worker is a little convoluted (I think
   necessarily) and is worth explaining. When [spawn_worker] is called, the executable is
   run in another process with information needed to connect back to the master process.
   If all goes well, the new process will send an Rpc back to the master with its own host
   and port information. The master will then connect to the worker and everything will be
   dandy.

   However, something could go wrong in the worker before it sends that Rpc to the
   master. There must be some timeout so the master doesn't wait on the worker connection
   forever. If the connection gets established after the master has already timed out, the
   worker should be told to shutdown. With all this happening asynchronously, there is a
   a little juggle going on with checking some Hashtables to see what has and hasn't
   happened. *)

module Make(
  M : sig
    type worker_arg [@@deriving bin_io]
    type worker_ret [@@deriving bin_io]

    val worker_main : worker_arg -> worker_ret Deferred.t
  end) = struct

  type worker_id = Worker_id.t

  type state =
    {
      (* Master host and port information *)
      master: Host_and_port.t;
      (* Used to facilitate timeout of connecting to a new worker *)
      pending: Rpc.Connection.t Or_error.t Ivar.t Worker_id.Table.t;
      (* Keep track of processes for connected local workers *)
      processes: Process.t Worker_id.Table.t;
      (* Connected workers *)
      workers: Rpc.Connection.t Worker_id.Table.t;
      (* Callbacks for worker exceptions and disconnects *)
      on_failures: (Error.t -> unit) Worker_id.Table.t
    }

  (* Must be an Ivar.t because there are no guarantees with the Async scheduler and
     [master_main] must run to completion before any calls to [spawn_worker] begin. The
     [Scheduler.go()] call occurs inside of the main command that is run. *)
  let glob : state Ivar.t = Ivar.create ()

  (* Environment variables *)

  let is_child_env_var                 = "ASYNC_PARALLEL_IS_CHILD_MACHINE"
  let env_vars = [is_child_env_var]

  type worker_register = Worker_id.t * Host_and_port.t [@@deriving bin_io]
  type worker_error = Worker_id.t * Error.t [@@deriving bin_io]

  (* [Rpc.t]'s implemented by master process *)
  let register_worker_rpc =
    Rpc.Rpc.create
      ~name:"register_worker_rpc"
      ~version:0
      ~bin_query:bin_worker_register
      ~bin_response:Unit.bin_t

  let handle_worker_exn_rpc =
    Rpc.Rpc.create
      ~name:"handle_worker_exn_rpc"
      ~version:0
      ~bin_query:bin_worker_error
      ~bin_response:Unit.bin_t

  (* [Rpc.t]'s implemented by worker processes *)
  let worker_run_rpc =
    Rpc.Rpc.create
      ~name:"worker_run_rpc"
      ~version:0
      ~bin_query:M.bin_worker_arg
      ~bin_response:M.bin_worker_ret

  module Shutdown_rpc = struct
    let rpc =
      Rpc.One_way.create
        ~name:"shutdown_rpc"
        ~version:0
        ~bin_msg:Unit.bin_t
    ;;

    let implementation = Rpc.One_way.implement rpc (fun () () -> Shutdown.shutdown 0)
    ;;

    let dispatch connection =
      match Rpc.One_way.dispatch rpc connection () with
      | Error _ as result -> return result
      | Ok () -> Rpc.Connection.close_finished connection >>| fun () -> Ok ()
    ;;
  end

  let validate_extra_env env =
    match List.find env ~f:(fun (key, _) -> List.mem env_vars key ~equal:String.equal) with
    | Some e ->
      Or_error.error
        "Environment variable conflicts with Rpc_parallel machinery"
        e [%sexp_of: string*string]
    | None -> Ok ()

  (* Run the previously copied over executable. Before doing so, set the appropriate
     environment variables. Demonize the process so that the ssh connection doesn't remain
     open unnecessarily *)
  let remote_cmd env bin_name mach_id =
    let open Or_error.Monad_infix in
    validate_extra_env env
    >>| fun () ->
    let cheesy_escape str = Sexp.to_string (String.sexp_of_t str) in
    let env =
      String.concat
        (List.map env ~f:(fun (key, data) -> key ^ "=" ^ cheesy_escape data ^ ""))
        ~sep:" "
    in
    sprintf "%s %s=\"%s\" %s"
      env
      is_child_env_var                 (Worker_id.to_string mach_id)
      bin_name

  (* Environment for a local worker *)
  let local_worker_env ~extra ~id =
    let open Or_error.Monad_infix in
    validate_extra_env extra
    >>| fun () ->
    `Extend (extra @ [is_child_env_var, Worker_id.to_string id])

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
  ;;

  (* Run the current executable either locally or remotely. For remote executables, make
     sure that the md5s match. *)
  let init_worker ?rpc_max_message_size ?rpc_handshake_timeout ?rpc_heartbeat_config
         ?cd ?umask ~redirect_stdout ~redirect_stderr
        ~env ~id ~where ~master ~disown =
    Ivar.read glob
    >>= fun st ->
    let binary = our_binary () in
    let worker_config =
      { Worker_config.
        disown
      ; master
      ; rpc_max_message_size
      ; rpc_handshake_timeout
      ; rpc_heartbeat_config
      ; cd
      ; umask
      ; redirect_stdout
      ; redirect_stderr
      }
    in
    match where with
    | `Local ->
      begin
        match local_worker_env ~extra:env ~id with
        | Error _ as e -> return e
        | Ok env ->
          Process.create ~prog:binary ~args:[] ~env ()
          >>| function
          | Error _ as err -> err
          | Ok p ->
            (* It is important that we start waiting for the child process here. The
               worker it forks daemonizes, thus this process ends. This will happen before
               the worker process quits or closes his RPC connection. If we only [wait]
               once the RPC connection is closed, we will have zombie processes hanging
               around until then. *)
            cleanup_standard_fds p;
            Hashtbl.add_exn st.processes ~key:id ~data:p;
            Writer.write_sexp (Process.stdin p) (Worker_config.sexp_of_t worker_config);
            Ok ()
      end
    | `Remote (exec:_ Remote_executable.t) ->
      our_md5 () >>= function
      | Error _ as err -> return err
      | Ok md5 ->
        Process.run ~prog:"ssh"
          ~args:(exec.host_key_checking @ [exec.host; "md5sum"; exec.path])
          ()
        >>= function
        | Error _ as err -> return err
        | Ok remote_md5 ->
          let remote_md5, _ = String.lsplit2_exn ~on:' ' remote_md5 in
          if md5 <> remote_md5 then
            return (Error (Error.of_string (sprintf
                                              "The remote executable %s does not match the local executable"
                                              exec.path)))
          else
            match remote_cmd env exec.path id with
            | Error _ as e -> return e
            | Ok remote_cmd ->
             Process.create ~prog:"ssh"
                ~args:(exec.host_key_checking @ [exec.host; remote_cmd]) ()
              >>| function
              | Error _ as err -> err
              | Ok p ->
                cleanup_standard_fds p;
                Writer.write_sexp (Process.stdin p) (Worker_config.sexp_of_t worker_config);
                Ok ()

  let spawn_worker
        ?(where=`Local) ?(disown=false) ?(env=[])
        ?rpc_max_message_size ?rpc_handshake_timeout ?rpc_heartbeat_config
        ?(connection_timeout=(sec 10.)) ?cd ?umask ~redirect_stdout ~redirect_stderr
        worker_arg ~on_failure =
    Ivar.read glob
    >>= fun st ->
    let id = Worker_id.create () in
    let host =
      match where with
      | `Local -> "local"
      | `Remote exec -> exec.Remote_executable.host
    in
    let pending_ivar = Ivar.create () in
    Hashtbl.add_exn st.pending ~key:id ~data:pending_ivar;
    Hashtbl.add_exn st.on_failures ~key:id ~data:on_failure;
    init_worker
      ?rpc_max_message_size ?rpc_handshake_timeout ?rpc_heartbeat_config
       ?cd ?umask ~redirect_stdout ~redirect_stderr
      ~id ~where ~master:st.master ~disown ~env
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
        Hashtbl.remove st.processes id;
        let error = Error.createf "Timed out getting connection from %s process" host in
        return (Error error)
      | `Result connection_or_error ->
        Hashtbl.remove st.pending id;
        match connection_or_error with
        | Ok connection ->
          begin
            Rpc.Rpc.dispatch worker_run_rpc connection worker_arg
            >>| function
            | Error _ as err ->
              Hashtbl.remove st.on_failures id;
              Hashtbl.remove st.processes id;
              err
            | Ok res ->
              Hashtbl.add_exn st.workers ~key:id ~data:connection;
              (* Setup cleanup *)
              (Rpc.Connection.close_reason ~on_close:`finished connection
               >>> fun info ->
               match Hashtbl.find st.workers id with
               | None ->
                 (* [kill_worker] was called, don't report closed connection *)
                 ()
               | Some _ ->
                 Hashtbl.remove st.workers id;
                 let error =
                   match Hashtbl.mem st.processes id with
                  | false ->
                    Error.createf !"Lost connection with worker: %{sexp:Info.t}" info
                  | true ->
                    Error.createf !"Local process exited: %{sexp:Info.t}" info
                 in
                 Hashtbl.remove st.processes id;
                 on_failure error);
              Ok (res, id)
          end
        | Error _ as err ->
          Hashtbl.remove st.on_failures id;
          Hashtbl.remove st.processes id;
          return err

  let spawn_worker_exn ?where ?disown ?env
        ?rpc_max_message_size ?rpc_handshake_timeout ?rpc_heartbeat_config
        ?connection_timeout
        ?cd ?umask ~redirect_stdout ~redirect_stderr
        worker_arg ~on_failure =
    spawn_worker ?where ?disown ?env
      ?rpc_max_message_size ?rpc_handshake_timeout ?rpc_heartbeat_config
      ?connection_timeout
      ?cd ?umask ~redirect_stdout ~redirect_stderr
      worker_arg ~on_failure
    >>| Or_error.ok_exn

  (* worker main function *)
  let worker_main ~id ~(config : Worker_config.t) ~release_daemon =
    let worker_run_implementation =
      Rpc.Rpc.implement worker_run_rpc (fun () worker_arg ->
        try_within_exn ~monitor:Monitor.main (fun () ->
          M.worker_main worker_arg))
    in
    let implementations =
      Rpc.Implementations.create_exn
        ~implementations:[worker_run_implementation; Shutdown_rpc.implementation]
        ~on_unknown_rpc:`Close_connection
    in
    Rpc.Connection.serve ~implementations ~initial_connection_state:(fun _ _ -> ())
      ?max_message_size:config.rpc_max_message_size
      ?handshake_timeout:config.rpc_handshake_timeout
      ?heartbeat_config:config.rpc_heartbeat_config
      ~where_to_listen:Tcp.on_port_chosen_by_os ()
    >>> fun serv ->
    let host_and_port =
      Host_and_port.create ~host:(Unix.gethostname ())
        ~port: (Tcp.Server.listening_on serv)
    in
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
        Rpc.Rpc.dispatch handle_worker_exn_rpc conn (id, Error.of_exn exn)
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
      ignore (Rpc.Rpc.dispatch_exn register_worker_rpc conn (id, host_and_port))

  (* setup everything needed to act as an rpc_parallel master. *)
  let setup_master ?rpc_max_message_size ?rpc_handshake_timeout
      ?rpc_heartbeat_config ?where_to_listen () =
    let pending = Worker_id.Table.create () in
    let processes = Worker_id.Table.create () in
    let workers = Worker_id.Table.create () in
    let on_failures = Worker_id.Table.create () in
    let register_worker_implementation =
      Rpc.Rpc.implement register_worker_rpc (fun () (id, worker_hp) ->
        Rpc.Connection.client
          ?max_message_size:rpc_max_message_size
          ?handshake_timeout:rpc_handshake_timeout
          ?heartbeat_config:rpc_heartbeat_config
          ~host:(Host_and_port.host worker_hp)
          ~port:(Host_and_port.port worker_hp) ()
        >>= function
        | Error e ->
          begin
            (* Failed to connect to worker, it must be dead *)
            match Hashtbl.find pending id with
            | None ->
              (* We already returned a failure to the [spawn_worker] caller *)
              return ()
            | Some ivar ->
              return (Ivar.fill ivar (Error (Error.of_exn e)))
          end
        | Ok connection ->
          match Hashtbl.find pending id with
          | None ->
            (* We already returned a failure to the [spawn_worker] caller,
               tell the worker to shutdown *)
            Shutdown_rpc.dispatch connection >>| ignore
          | Some ivar ->
            (* We are connected and haven't timed out *)
            return (Ivar.fill ivar (Ok connection)))
    in
    let handle_worker_exn_implementation =
      Rpc.Rpc.implement handle_worker_exn_rpc (fun () (id, error) ->
        let on_failure = Hashtbl.find_exn on_failures id in
        on_failure error;
        return ())
    in
    let implementations =
      Rpc.Implementations.create_exn
        ~implementations:[register_worker_implementation;
                          handle_worker_exn_implementation]
        ~on_unknown_rpc:`Close_connection
    in
    (Rpc.Connection.serve ~implementations ~initial_connection_state:(fun _ _ -> ())
       ?max_message_size:rpc_max_message_size
       ?handshake_timeout:rpc_handshake_timeout
       ?heartbeat_config:rpc_heartbeat_config
       ~where_to_listen:(Option.value where_to_listen ~default:Tcp.on_port_chosen_by_os)
       ()
     >>> fun serv ->
     let master_host_and_port =
       Host_and_port.create ~host:(Unix.gethostname())
         ~port:(Tcp.Server.listening_on serv)
     in
     Ivar.fill glob {master=master_host_and_port; pending; workers; processes;
                     on_failures})

  let kill_worker worker =
    Ivar.read glob
    >>= fun st ->
    match Hashtbl.find st.workers worker with
    | None ->
      return
        (Or_error.error_string
           "Worker already dead or was spawned from some other process")
    | Some connection ->
      Hashtbl.remove st.workers worker;
      Hashtbl.remove st.processes worker;
      Shutdown_rpc.dispatch connection

  let parse_worker_config () =
    match Sys.getenv is_child_env_var with
    | Some id_str ->
      let config_sexp = Sexp.input_sexp In_channel.stdin in
      let id = Worker_id.of_string id_str in
      `Worker (id, Worker_config.t_of_sexp config_sexp)
    | None -> `Master

  let clear_env_vars () =
    List.iter [is_child_env_var] ~f:Unix.unsetenv

  let run ?rpc_max_message_size ?rpc_handshake_timeout
      ?rpc_heartbeat_config ?where_to_listen command =
    setup_master
      ?rpc_max_message_size ?rpc_handshake_timeout
      ?rpc_heartbeat_config ?where_to_listen ();
    let worker_config = parse_worker_config () in
    clear_env_vars ();
    match worker_config with
    | `Worker (id, config) ->
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
    | `Master ->
      Command.run command
end
