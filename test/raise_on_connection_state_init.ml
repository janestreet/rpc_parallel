open Core
open Async

module Worker = struct
  module T = struct
    type 'worker functions = unit

    module Worker_state = struct
      type init_arg = unit [@@deriving bin_io]
      type t = unit
    end

    module Connection_state = struct
      type init_arg = unit [@@deriving bin_io]
      type t = unit
    end

    module Functions (_ : Rpc_parallel.Creator) = struct
      let init_connection_state ~connection:_ ~worker_state:_ () =
        failwith "text of expected failure"
      ;;

      let init_worker_state () = Deferred.unit
      let functions = ()
    end
  end

  include Rpc_parallel.Make (T)
end

let command =
  let open Command.Let_syntax in
  Command.async
    ~summary:"testing worker shutdown"
    (let%map () = return () in
     fun () ->
       let open Deferred.Let_syntax in
       let%bind worker =
         Worker.spawn_exn
           ~redirect_stdout:`Dev_null
           ~redirect_stderr:`Dev_null
           ~shutdown_on:Called_shutdown_function
           ~on_failure:Error.raise
           ()
       in
       match%bind Worker.Connection.client worker () with
       | Ok (_ : Worker.Connection.t) -> failwith "expected to fail but did not"
       | Error e ->
         printf !"expected failure:\n%{sexp:Error.t}\n" e;
         let%bind () = Worker.shutdown worker >>| ok_exn in
         Deferred.unit)
    ~behave_nicely_in_pipeline:false
;;

let () = Rpc_parallel_krb_public.start_app ~krb_mode:For_unit_test command
