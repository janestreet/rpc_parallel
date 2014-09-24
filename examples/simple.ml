open Core.Std
open Async.Std
open Rpc_parallel.Std

(* A bare bones use case of the [Rpc_parallel] library. This demonstrates how to
   define a simple worker type that implements some functions. The master then spawns a
   worker of this type and calls a function to run on this worker *)

module Sum_worker = struct
  module T = struct
    (* A [Sum_worker.t] implements a single function [sum : int -> int]. Because this
       function is parameterized on a ['worker], it can only be run on workers of the
       [Sum_worker.t] type. *)
    type 'worker functions = {sum:('worker, int, int) Parallel.Function.t}

    (* A [Sum_worker.t] doesn't have any initialization upon [spawn] *)
    type init_arg = unit with bin_io
    let init = return

    module Functions(C:Parallel.Creator) = struct
      (* Define the implementation for the [sum] function *)
      let sum_impl arg =
        let sum = List.fold ~init:0 ~f:(+) (List.init arg ~f:Fn.id) in
        return sum

      (* Create a [Parallel.Function.t] from the above implementation *)
      let sum = C.create_rpc ~f:sum_impl ~bin_input:Int.bin_t ~bin_output:Int.bin_t ()

      (* This type must match the ['worker functions] type defined above *)
      let functions = {sum}
    end
  end
  include Parallel.Make_worker(T)
end

let command =
  (* Make sure to always use [Command.async] *)
  Command.async ~summary:"Simple use of Async Parallel V2"
    Command.Spec.(
      empty
      +> flag "max" (required int) ~doc:""
    )
    (fun max () ->
       (* This is the main function called in the master. Spawn a local worker and run
          the [sum] function on this worker *)
       Sum_worker.spawn_exn () ~on_failure:Error.raise >>= fun sum_worker ->
       Sum_worker.run_exn sum_worker ~f:Sum_worker.functions.sum ~arg:max >>= fun res ->
       return (Printf.printf "sum_worker: %d\n%!" res))

(* This call to [Parallel.start_app] must be top level *)
let () = Parallel.start_app command
