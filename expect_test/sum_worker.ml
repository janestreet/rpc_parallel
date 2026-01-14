open Core
open Async

(* A bare bones use case of the [Rpc_parallel] library. This demonstrates how to define a
   simple worker type that implements some functions. The master then spawns a worker of
   this type and calls a function to run on this worker *)

module T = struct
  (* A [Sum_worker.worker] implements a single function [sum : int -> int]. Because this
     function is parameterized on a ['worker], it can only be run on workers of the
     [Sum_worker.worker] type. *)
  type 'worker functions = { sum : ('worker, int, int) Rpc_parallel.Function.t }

  (* No initialization upon spawn *)
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
    (* Define the implementation for the [sum] function *)
    let sum_impl ~worker_state:() ~conn_state:() arg =
      let sum = List.fold ~init:0 ~f:( + ) (List.init arg ~f:Fn.id) in
      [%log.info_format "Sum_worker.sum: %i\n" sum];
      return sum
    ;;

    (* Create a [Rpc_parallel.Function.t] from the above implementation *)
    let sum = C.create_rpc ~f:sum_impl ~bin_input:Int.bin_t ~bin_output:Int.bin_t ()

    (* This type must match the ['worker functions] type defined above *)
    let functions = { sum }
    let init_worker_state () = Deferred.unit
    let init_connection_state ~connection:_ ~worker_state:_ = return
  end
end

include Rpc_parallel.Make (T)

let main max =
  let%bind conn =
    (* This is the main function called in the master. Spawn a local worker and run the
       [sum] function on this worker *)
    spawn
      ~on_failure:Error.raise
      ~shutdown_on:Connection_closed
      ~redirect_stdout:`Dev_null
      ~redirect_stderr:`Dev_null
      ~connection_state_init_arg:()
      ()
    >>| ok_exn
  in
  Connection.run_exn conn ~f:functions.sum ~arg:max
;;

let () = Rpc_parallel_krb_public.For_testing.initialize [%here]

let%expect_test "" =
  let%bind res = main 42 in
  printf "%d\n" res;
  [%expect {| 861 |}];
  return ()
;;
