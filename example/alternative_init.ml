open Core.Std
open Async.Std
open Rpc_parallel.Std

module Sum_worker = struct
  module T = struct
    type 'worker functions = {sum:('worker, int, int) Parallel.Function.t}

    module Worker_state = struct
      type init_arg = unit [@@deriving bin_io]
      type t = unit
    end

    module Connection_state = struct
      type init_arg = unit [@@deriving bin_io]
      type t = unit
    end

    module Functions
        (C : Parallel.Creator
         with type worker_state := Worker_state.t
          and type connection_state := Connection_state.t) = struct
      let sum_impl ~worker_state:() ~conn_state:() arg =
        let sum = List.fold ~init:0 ~f:(+) (List.init arg ~f:Fn.id) in
        printf "Sum_worker.sum: %i\n" sum;
        return sum

      let sum = C.create_rpc ~f:sum_impl ~bin_input:Int.bin_t ~bin_output:Int.bin_t ()

      let functions = {sum}

      let init_worker_state ~parent_heartbeater () =
        printf "Sum_worker.init\n";
        Parallel.Heartbeater.(if_spawned connect_and_shutdown_on_disconnect_exn)
          parent_heartbeater
        >>| fun ( `Connected | `No_parent ) -> ()

      let init_connection_state ~connection:_ ~worker_state:_ = return
    end
  end
  include Parallel.Make(T)
end

let command =
  Command.group
    ~summary:"Simple use of Async Parallel V2"
    [ "worker",
      Command.basic
        ~summary:"for internal use"
        Command.Spec.empty
        (fun () -> never_returns (Parallel.Expert.run_as_worker_exn ()))
    ; "main",
      Command.async_or_error
        ~summary:"start the master and spawn the workers"
        Command.Spec.(
          empty
          +> flag "max" (required int) ~doc:"NUM what number to add up to"
          +> flag "log-dir" (optional string)
               ~doc:" Folder to write worker logs to")
        (fun max log_dir () ->
           Parallel.Expert.init_master_exn ~worker_command_args:["worker"] ();
           let redirect_stdout, redirect_stderr =
             match log_dir with
             | None -> (`Dev_null, `Dev_null)
             | Some _ -> (`File_append "sum.out", `File_append "sum.err")
           in
           Sum_worker.spawn ~on_failure:Error.raise
             ?cd:log_dir ~redirect_stdout ~redirect_stderr ()
           >>=? fun sum_worker ->
           Sum_worker.Connection.client sum_worker ()
           >>=? fun conn ->
           Sum_worker.Connection.run conn ~f:Sum_worker.functions.sum ~arg:max
           >>|? fun res ->
           Core.Std.Printf.printf "sum_worker: %d\n%!" res
        )
    ]

let () = Command.run command
