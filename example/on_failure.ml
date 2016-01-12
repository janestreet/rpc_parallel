open Core.Std
open Async.Std
open Rpc_parallel.Std

module Sum_worker = struct
  module T = struct
    type 'worker functions = {sum:('worker, int, int) Parallel.Function.t}

    type init_arg = unit [@@deriving bin_io]
    type state = unit

    let init () =
      don't_wait_for (Clock.after (sec 5.) >>= fun () -> exit 0);
      Deferred.unit

    module Functions(C:Parallel.Creator with type state := state) = struct
      let sum_impl () arg =
        let sum = List.fold ~init:0 ~f:(+) (List.init arg ~f:Fn.id) in
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
    )
    (fun () ->
       Monitor.try_with ~name:"correct monitor" (fun () ->
         Sum_worker.spawn_exn ~redirect_stdout:`Dev_null
           ~redirect_stderr:`Dev_null () ~on_failure:Error.raise
         >>= fun _worker ->
         Clock.after (sec 10.))
       >>| function
       | Ok () ->
         failwith "The exception was either not raised or was caught by the wrong monitor"
       | Error exn ->
         printf !"%{Exn}\n" exn)

let () = Parallel.start_app command
