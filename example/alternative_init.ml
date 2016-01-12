open Core.Std
open Async.Std
open Rpc_parallel.Std

module Sum_worker = struct
  module T = struct
    type 'worker functions = {sum:('worker, int, int) Parallel.Function.t}

    type init_arg = unit [@@deriving bin_io]
    type state = unit

    let init () =
      printf "Sum_worker.init\n";
      return ()

    module Functions(C:Parallel.Creator with type state := state) = struct
      let sum_impl () arg =
        let sum = List.fold ~init:0 ~f:(+) (List.init arg ~f:Fn.id) in
        printf "Sum_worker.sum: %i\n" sum;
        return sum

      let sum = C.create_rpc ~f:sum_impl ~bin_input:Int.bin_t ~bin_output:Int.bin_t ()

      let functions = {sum}
    end
  end
  include Parallel.Make_worker(T)
end

let command =
  Command.async ~summary:"Simple use of Async Parallel V2"
    Command.Spec.(
      empty
      +> flag "max" (required int) ~doc:"NUM what number to add up to"
      +> flag "log-dir" (optional string)
           ~doc:" Folder to write worker logs to"
      +> flag "as-worker" no_arg ~doc:"Run as a worker (Should not be called directly)"
    )
    (fun max log_dir as_worker () ->
       if as_worker then
         never_returns
           (Parallel.Expert.run_as_worker_exn
              ~worker_command_args:["-max"; "10"; "-as-worker"])
       else begin
         Parallel.Expert.init_master_exn
           ~worker_command_args:["-max"; "10"; "-as-worker"] ();
         let redirect_stdout, redirect_stderr =
           match log_dir with
           | None -> (`Dev_null, `Dev_null)
           | Some _ -> (`File_append "sum.out", `File_append "sum.err")
         in
         Sum_worker.spawn_exn ~on_failure:Error.raise
           ?cd:log_dir ~redirect_stdout ~redirect_stderr ()
         >>= fun sum_worker ->
         Sum_worker.run_exn sum_worker ~f:Sum_worker.functions.sum ~arg:max >>= fun res ->
         return (Core.Std.Printf.printf "sum_worker: %d\n%!" res)
       end)

let () = Command.run command
