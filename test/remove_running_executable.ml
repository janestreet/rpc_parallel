open Core
open Async

module Worker_impl = struct
  type 'a functions = unit

  module Worker_state = struct
    type init_arg = unit [@@deriving bin_io]
    type t = unit
  end

  module Connection_state = struct
    type init_arg = unit [@@deriving bin_io]
    type t = unit
  end

  module Functions
      (_ : Rpc_parallel.Creator
           with type worker_state := Worker_state.t
            and type connection_state := Connection_state.t) =
  struct
    let functions = ()
    let init_worker_state () = Deferred.unit
    let init_connection_state ~connection:_ ~worker_state:_ = return
  end
end

module Worker = Rpc_parallel.Make (Worker_impl)

let with_rename f =
  (* Force the lazy because `readlink /proc/PID/exe` changes when you rename the
     executable *)
  let%bind _worker =
    Worker.spawn_exn
      ~on_failure:Error.raise
      ~shutdown_on:Heartbeater_connection_timeout
      ~redirect_stdout:`Dev_null
      ~redirect_stderr:`Dev_null
      ()
  in
  (* move the currently running executable *)
  let%bind cwd = Unix.getcwd () in
  let old_path =
    if Filename.is_absolute Sys.executable_name
    then Sys.executable_name
    else cwd ^/ Sys.executable_name
  in
  let new_path = sprintf "%s.bak" old_path in
  let%bind () = Unix.rename ~src:old_path ~dst:new_path in
  (* run f *)
  match%map
    Monitor.protect ~run:`Schedule ~rest:`Log f ~finally:(fun () ->
      Unix.rename ~src:new_path ~dst:old_path)
  with
  | Ok () -> printf "Ok.\n"
  | Error e -> Error.raise e
;;

let spawn_local () =
  with_rename (fun () ->
    Worker.spawn
      ~on_failure:Error.raise
      ~shutdown_on:Heartbeater_connection_timeout
      ~redirect_stdout:`Dev_null
      ~redirect_stderr:`Dev_null
      ()
    >>|? fun _worker -> ())
;;

let spawn_remote () =
  with_rename (fun () ->
    let%bind cwd = Unix.getcwd () in
    let host = Unix.gethostname () in
    Rpc_parallel.Remote_executable.copy_to_host
      ~executable_dir:cwd
      host
      ~strict_host_key_checking:`No
    >>=? fun executable ->
    Worker.spawn
      ~on_failure:Error.raise
      ~shutdown_on:Heartbeater_connection_timeout
      ~redirect_stdout:`Dev_null
      ~redirect_stderr:`Dev_null
      ~how:(Rpc_parallel.How_to_run.remote executable)
      ()
    >>=? fun _worker -> Rpc_parallel.Remote_executable.delete executable)
;;

let command =
  let open Command.Let_syntax in
  Command.async
    ~summary:""
    (let%map_open local = flag "spawn-local" no_arg ~doc:" local spawn"
     and remote = flag "spawn-remote" no_arg ~doc:" remote spawn (on local machine)" in
     fun () ->
       match local, remote with
       | true, true | false, false -> failwith "specify -spawn-local or -spawn-remote"
       | true, false -> spawn_local ()
       | false, true -> spawn_remote ())
    ~behave_nicely_in_pipeline:false
;;

let () = Rpc_parallel_krb_public.start_app ~krb_mode:For_unit_test command
