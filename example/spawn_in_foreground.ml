
open Core.Std
open Async.Std
open Rpc_parallel.Std

module Worker = struct
  module T = struct
    type 'worker functions = {print:('worker, string, unit) Parallel.Function.t}

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
      let print_impl ~worker_state:() ~conn_state:() string =
        printf "%s\n" string;
        return ()

      let print = C.create_rpc ~f:print_impl ~bin_input:String.bin_t ~bin_output:Unit.bin_t ()

      let functions = {print}

      let init_worker_state ~parent_heartbeater () =
        Parallel.Heartbeater.(if_spawned connect_and_shutdown_on_disconnect_exn)
          parent_heartbeater
        >>| fun ( `Connected | `No_parent ) -> ()

      let init_connection_state ~connection:_ ~worker_state:_ = return
    end
  end
  include Parallel.Make(T)
end

let main () =
  Worker.spawn_in_foreground ~on_failure:Error.raise ()
  >>=? fun (worker, process) ->
  Worker.Connection.client worker ()
  >>=? fun conn ->
  Worker.Connection.run conn ~f:Worker.functions.print ~arg:"HELLO"
  >>=? fun () ->
  Worker.Connection.run conn ~f:Worker.functions.print ~arg:"HELLO2"
  >>=? fun () ->
  Worker.Connection.run conn ~f:Parallel.Function.shutdown ~arg:()
  >>=? fun () ->
  Process.wait process
  >>= fun (_ : Unix.Exit_or_signal.t) ->
  let worker_stderr = Reader.lines (Process.stderr process) in
  let worker_stdout = Reader.lines (Process.stdout process) in
  Pipe.iter worker_stderr ~f:(fun line ->
    let line' = sprintf "[WORKER STDERR]: %s\n" line in
    Writer.write (Lazy.force Writer.stdout) line' |> return)
  >>= fun () ->
  Pipe.iter worker_stdout ~f:(fun line ->
    let line' = sprintf "[WORKER STDOUT]: %s\n" line in
    Writer.write (Lazy.force Writer.stdout) line' |> return)
  >>= fun () ->
  Deferred.Or_error.ok_unit

let command =
  Command.async_or_error ~summary:"Example of spawn_in_foreground"
    Command.Spec.empty
    main

let () = Parallel.start_app command
