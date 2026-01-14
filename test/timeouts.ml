open Core
open Async
module Rpc_settings = Rpc_parallel.Rpc_settings

module Unresponsive_worker = struct
  module T = struct
    (* An [Unresponsive_worker.t] implements a single function [wait : int -> unit]. It
       waits for the specified number of seconds (blocking Async) and then returns (). *)
    type 'worker functions = { wait : ('worker, int, unit) Rpc_parallel.Function.t }

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
      let wait_impl ~worker_state:() ~conn_state:() seconds =
        Core_unix.sleep seconds;
        return ()
      ;;

      let wait = C.create_rpc ~f:wait_impl ~bin_input:Int.bin_t ~bin_output:Unit.bin_t ()
      let functions = { wait }
      let init_worker_state () = Deferred.unit
      let init_connection_state ~connection:_ ~worker_state:_ = return
    end
  end

  include Rpc_parallel.Make (T)
end

let timeout_command =
  Command.async_spec
    ~summary:"Exercise timeouts in Rpc parallel"
    Command.Spec.(empty +> flag "sleep-for" (required int) ~doc:"")
    (fun sleep_for () ->
      let%bind conn =
        Unresponsive_worker.spawn_exn
          ~shutdown_on:Connection_closed
          ~redirect_stdout:`Dev_null
          ~redirect_stderr:`Dev_null
          ~on_failure:(fun e -> Error.raise (Error.tag e ~tag:"spawn_exn"))
          ~connection_state_init_arg:()
          ()
      in
      match%map
        Unresponsive_worker.Connection.run
          conn
          ~f:Unresponsive_worker.functions.wait
          ~arg:sleep_for
      with
      | Error e -> printf !"%{sexp:Error.t}\n" e
      | Ok () -> printf "unresponsive worker returned\n")
    ~behave_nicely_in_pipeline:false
;;

let report_rpc_settings_command =
  Command.async
    ~summary:"Test for rpc_settings alignment"
    (let%map_open.Command () = return () in
     fun () ->
       let%bind spawned_worker =
         Unresponsive_worker.spawn_exn
           ~shutdown_on:Heartbeater_connection_timeout
           ~redirect_stdout:`Dev_null
           ~redirect_stderr:`Dev_null
           ~on_failure:(fun e -> Error.raise (Error.tag e ~tag:"spawn_exn"))
           ()
       in
       let%bind served_worker = Unresponsive_worker.serve () in
       let master_rpc_settings =
         Unresponsive_worker.For_internal_testing.master_app_rpc_settings ()
       in
       printf !"master : %{sexp:Rpc_settings.t}\n" master_rpc_settings;
       let print_worker_rpc_settings ~which worker =
         let%bind worker_conn = Unresponsive_worker.Connection.client_exn worker () in
         let worker_client_side_rpc_settings = Unresponsive_worker.rpc_settings worker in
         let%bind worker_server_side_rpc_settings =
           Unresponsive_worker.Connection.run_exn
             worker_conn
             ~f:Rpc_parallel.Function.For_internal_testing.worker_server_rpc_settings
             ~arg:()
         in
         printf
           !"%s worker (client side) : %{sexp:Rpc_settings.t}\n"
           which
           worker_client_side_rpc_settings;
         printf
           !"%s worker (server side) : %{sexp:Rpc_settings.t}\n"
           which
           worker_server_side_rpc_settings;
         return ()
       in
       let%bind () = print_worker_rpc_settings ~which:"spawned" spawned_worker in
       let%bind () = print_worker_rpc_settings ~which:"served" served_worker in
       return ())
    ~behave_nicely_in_pipeline:false
;;

let app_rpc_settings =
  Rpc_settings.For_internal_testing.create_with_env_override
    ~env_var:"APP_RPC_SETTINGS_FOR_TEST"
    ~max_message_size:None
    ~buffer_age_limit:None
    ~handshake_timeout:None
    ~heartbeat_config:None
;;

let () =
  let { Rpc_settings.max_message_size = rpc_max_message_size
      ; buffer_age_limit = rpc_buffer_age_limit
      ; handshake_timeout = rpc_handshake_timeout
      ; heartbeat_config = rpc_heartbeat_config
      }
    =
    app_rpc_settings
  in
  Command.group
    ~summary:"timeout and rpc-heartbeat testing commands"
    [ "timeout", timeout_command; "rpc-settings", report_rpc_settings_command ]
  |> Rpc_parallel_krb_public.start_app
       ?rpc_max_message_size
       ?rpc_buffer_age_limit
       ?rpc_handshake_timeout
       ?rpc_heartbeat_config
       ~krb_mode:For_unit_test
;;
