open Core
open Async

module T = struct
  type 'worker functions = { product : ('worker, int, int) Rpc_parallel.Function.t }

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
    let product_impl ~worker_state:() ~conn_state:() arg =
      let product = List.fold ~init:1 ~f:( * ) (List.init arg ~f:(( + ) 1)) in
      [%log.info_format "Prod_worker.product: %i\n" product];
      return product
    ;;

    let product =
      C.create_rpc ~f:product_impl ~bin_input:Int.bin_t ~bin_output:Int.bin_t ()
    ;;

    let functions = { product }
    let init_worker_state () = Deferred.unit
    let init_connection_state ~connection:_ ~worker_state:_ = return
  end
end

include Rpc_parallel.Make (T)

let main max =
  let%bind conn =
    spawn
      ~on_failure:Error.raise
      ~shutdown_on:Connection_closed
      ~redirect_stdout:`Dev_null
      ~redirect_stderr:`Dev_null
      ~connection_state_init_arg:()
      ()
    >>| ok_exn
  in
  Connection.run_exn conn ~f:functions.product ~arg:max
;;

let () = Rpc_parallel_krb_public.For_testing.initialize [%here]

let%expect_test "" =
  let%bind res = main 10 in
  printf "%d\n" res;
  [%expect {| 3628800 |}];
  return ()
;;
