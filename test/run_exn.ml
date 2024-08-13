open Core
open Async

module Failer_impl = struct
  type 'a functions = ('a, unit, unit) Rpc_parallel.Function.t

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
    let functions =
      C.create_rpc
        ~bin_input:Unit.bin_t
        ~bin_output:Unit.bin_t
        ~f:(fun ~worker_state:() ~conn_state:() () -> failwith "text of expected failure")
        ()
    ;;

    let init_worker_state () = Deferred.unit
    let init_connection_state ~connection:_ ~worker_state:_ = return
  end
end

module Failer = Rpc_parallel.Make (Failer_impl)

let command =
  Command.async_spec_or_error
    ~summary:"ensure that raising in a worker function passes the exception to the master"
    Command.Spec.empty
    (fun () ->
      let%bind conn =
        Failer.spawn_exn
          ~on_failure:Error.raise
          ~shutdown_on:Connection_closed
          ~redirect_stdout:`Dev_null
          ~redirect_stderr:`Dev_null
          ~connection_state_init_arg:()
          ()
      in
      match%bind Failer.Connection.run conn ~f:Failer.functions ~arg:() with
      | Ok () -> failwith "expected to fail but did not"
      | Error e ->
        printf !"expected failure:\n%{sexp:Error.t}\n" e;
        Deferred.Or_error.ok_unit)
    ~behave_nicely_in_pipeline:false
;;

let () = Rpc_parallel_krb_public.start_app ~krb_mode:For_unit_test command
