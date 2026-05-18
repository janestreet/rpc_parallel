open Core
open Async

module Worker = struct
  module T = struct
    type 'worker functions = { ping : ('worker, unit, string) Rpc_parallel.Function.t }

    module Worker_state = struct
      type init_arg = unit [@@deriving bin_io]
      type t = unit
    end

    module Connection_state = struct
      type init_arg = unit [@@deriving bin_io]
      type t = unit
    end

    module Functions
        (C : Rpc_parallel.Creator
             with type worker_state := Worker_state.t
              and type connection_state := Connection_state.t) =
    struct
      let ping =
        C.create_rpc
          ~f:(fun ~worker_state:() ~conn_state:() () -> return "pong")
          ~bin_input:Unit.bin_t
          ~bin_output:String.bin_t
          ()
      ;;

      let functions = { ping }
      let init_worker_state () = Deferred.unit
      let init_connection_state ~connection:_ ~worker_state:_ = return
    end
  end

  include Rpc_parallel.Make (T)
end

let main ~host ~name =
  let open Deferred.Or_error.Let_syntax in
  let executable_dir = Filename.temp_dir_name in
  let%bind remote_exec =
    Rpc_parallel.Remote_executable.copy_to_host
      ~strict_host_key_checking:`No
      ~executable_dir
      host
  in
  let how = Rpc_parallel.How_to_run.remote remote_exec in
  let%bind conn =
    Worker.spawn
      ~how
      ~name
      ~shutdown_on:Connection_closed
      ~redirect_stdout:`Dev_null
      ~redirect_stderr:`Dev_null
      ~on_failure:Error.raise
      ~connection_state_init_arg:()
      ()
  in
  let%bind response = Worker.Connection.run conn ~f:Worker.functions.ping ~arg:() in
  printf "Worker with name %S responded: %s\n" name response;
  let%bind () = Deferred.ok (Worker.Connection.close conn) in
  let%bind () = Rpc_parallel.Remote_executable.delete remote_exec in
  return ()
;;

let () =
  Command.async_or_error
    ~summary:
      "Test that SSH remote workers can be spawned with names containing spaces and \
       parentheses"
    (let%map_open.Command host =
       flag
         "host"
         (optional_with_default "localhost" string)
         ~doc:"HOST host to connect to (default: localhost)"
     in
     fun () ->
       let names =
         [ "worker with spaces"
         ; "worker (with parens)"
         ; "worker (with spaces and parens)"
         ; "worker with 'single quotes'"
         ]
       in
       Deferred.Or_error.List.iter names ~how:`Sequential ~f:(fun name ->
         printf "Spawning worker with name: %S\n" name;
         main ~host ~name))
    ~behave_nicely_in_pipeline:false
  |> Rpc_parallel_krb_public.start_app ~krb_mode:For_unit_test
;;
