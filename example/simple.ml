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
    type init_arg = unit [@@deriving bin_io]
    type state = unit

    let init () =
      (* See comment in [Parallel.mli], this log message will not be sent to the master *)
      Log.Global.info "Sum_worker.init\n";
      return ()

    module Functions(C:Parallel.Creator with type state := state) = struct
      (* Define the implementation for the [sum] function *)
      let sum_impl () arg =
        let sum = List.fold ~init:0 ~f:(+) (List.init arg ~f:Fn.id) in
        Log.Global.info "Sum_worker.sum: %i\n" sum;
        return sum

      (* Create a [Parallel.Function.t] from the above implementation *)
      let sum = C.create_rpc ~f:sum_impl ~bin_input:Int.bin_t ~bin_output:Int.bin_t ()

      (* This type must match the ['worker functions] type defined above *)
      let functions = {sum}
    end
  end
  include Parallel.Make_worker(T)
end

let main max log_dir () =
  let redirect_stdout, redirect_stderr =
    match log_dir with
    | None -> (`Dev_null, `Dev_null)
    | Some _ -> (`File_append "sum.out", `File_append "sum.err")
  in
  (* This is the main function called in the master. Spawn a local worker and run
     the [sum] function on this worker *)
  Sum_worker.spawn ~on_failure:Error.raise
    ?cd:log_dir ~redirect_stdout ~redirect_stderr ()
  >>=? fun sum_worker ->
  Sum_worker.async_log sum_worker
  >>=? fun log ->
  don't_wait_for(Pipe.iter log ~f:(fun line ->
    printf !"%{sexp:Log.Message.Stable.V2.t}\n" line |> return));
  Sum_worker.run sum_worker ~f:Sum_worker.functions.sum ~arg:max
  >>|? fun res ->
  printf "sum_worker: %d\n%!" res

let command =
  (* Make sure to always use [Command.async] *)
  Command.async ~summary:"Simple use of Async Parallel V2"
    Command.Spec.(
      empty
      +> flag "max" (required int) ~doc:""
      +> flag "log-dir" (optional string)
           ~doc:" Folder to write worker logs to"
    )
    (fun max log_dir () ->
       main max log_dir () >>| Or_error.ok_exn)

(* This call to [Parallel.start_app] must be top level *)
let () = Parallel.start_app command
