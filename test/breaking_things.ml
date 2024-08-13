open Core
open Async

module Worker = struct
  module T = struct
    type 'worker functions =
      { add_one : unit -> ('worker, int, int) Rpc_parallel.Function.t }

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
      let add_one () =
        C.create_rpc
          ~f:(fun ~worker_state:() ~conn_state:() n -> return (n + 1))
          ~bin_input:Int.bin_t
          ~bin_output:Int.bin_t
          ()
      ;;

      let functions = { add_one }
      let init_worker_state () = Deferred.unit
      let init_connection_state ~connection:_ ~worker_state:_ = return
    end
  end

  include Rpc_parallel.Make (T)
end

let main () =
  Worker.spawn
    ~on_failure:Error.raise
    ~shutdown_on:Connection_closed
    ~redirect_stdout:`Dev_null
    ~redirect_stderr:`Dev_null
    ~connection_state_init_arg:()
    ()
  >>=? fun worker_conn ->
  Worker.Connection.run worker_conn ~f:(Worker.functions.add_one ()) ~arg:10
  >>=? fun res ->
  Core.Printf.printf "worker: %d\n%!" res;
  Deferred.Or_error.ok_unit
;;

let command =
  Command.async_spec_or_error
    ~summary:""
    Command.Spec.empty
    main
    ~behave_nicely_in_pipeline:false
;;

let () = Rpc_parallel_krb_public.start_app ~krb_mode:For_unit_test command
