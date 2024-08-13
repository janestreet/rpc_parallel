open Core
open Async

module T = struct
  type 'worker functions = { f : ('worker, unit, unit) Rpc_parallel.Function.t }

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
    let f_impl ~worker_state:() ~conn_state:() () = return ()

    let f =
      C.create_rpc
        ~f:f_impl
        ~bin_input:[%bin_type_class: unit]
        ~bin_output:[%bin_type_class: unit]
        ()
    ;;

    let functions = { f }
    let init_worker_state () = Deferred.unit
    let init_connection_state ~connection:_ ~worker_state:_ = return
  end
end

include Rpc_parallel.Make (T)

let main_command =
  let open Command.Let_syntax in
  Command.async
    ~summary:"start the master and spawn a worker"
    (let%map_open () = return () in
     fun () ->
       let open Deferred.Let_syntax in
       Rpc_parallel_krb_public.Expert.start_master_server_exn
         ~krb_mode:For_unit_test
         ~worker_command_args:[ "worker" ]
         ();
       let%bind conn =
         spawn_exn
           ~on_failure:Error.raise
           ~shutdown_on:Connection_closed
           ~connection_state_init_arg:()
           ~redirect_stdout:`Dev_null
           ~redirect_stderr:`Dev_null
           ()
       in
       let%map () = Connection.run_exn conn ~f:functions.f ~arg:() in
       print_endline "Success.")
    ~behave_nicely_in_pipeline:false
;;

let command =
  Command.group
    ~summary:"Using Rpc_parallel.Expert"
    [ "worker", Rpc_parallel_krb_public.Expert.worker_command; "main", main_command ]
;;

let () = Command_unix.run command
