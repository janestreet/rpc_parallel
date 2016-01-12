open Core.Std
open Async.Std
open Rpc_parallel.Std

(* A bare bones use case of the [Rpc_parallel] library. This exercises the timeouts in the
   API.*)

module Unresponsive_worker = struct
  module T = struct
    (* An [Unresponsive_worker.t] implements a single function [wait : int ->
       unit]. It waits for the specified number of seconds (blocking Async) and then
       returns (). *)
    type 'worker functions = {wait:('worker, int, unit) Parallel.Function.t}

    type init_arg = unit [@@deriving bin_io]
    type state = unit
    let init = return

    module Functions(C:Parallel.Creator with type state := state) = struct
      (* Define the implementation for the [wait] function *)
      let wait_impl () seconds =
        Core.Std.Unix.sleep seconds;
        return ()

      (* Create a [Parallel.Function.t] from the above implementation *)
      let wait = C.create_rpc ~f:wait_impl ~bin_input:Int.bin_t ~bin_output:Unit.bin_t ()

      (* This type must match the ['worker functions] type defined above *)
      let functions = {wait}
    end
  end
  include Parallel.Make_worker(T)
end

let command =
  (* Make sure to always use [Command.async] *)
  Command.async ~summary:"Exercise timeouts in Rpc parallel"
    Command.Spec.(
      empty
      +> flag "sleep-for" (required int) ~doc:""
      +> flag "timeout" (required int) ~doc:""
    )
    (fun sleep_for timeout_int () ->
       (* This is the main function called in the master. Spawn a local worker and run
          the [wait] function on this worker *)
       let timeout = Time_ns.Span.of_sec (Float.of_int timeout_int) in
       let heartbeat_config =
         let send_every = Time_ns.Span.of_sec (Float.of_int (timeout_int / 3)) in
         Rpc.Connection.Heartbeat_config.create ~timeout ~send_every
       in
       Unresponsive_worker.spawn_exn
         ~redirect_stdout:`Dev_null
         ~redirect_stderr:`Dev_null
         ()
         ~rpc_handshake_timeout:(Time_ns.Span.to_span timeout)
         ~rpc_heartbeat_config:heartbeat_config
         ~on_failure:(fun e -> Error.raise (Error.tag e "spawn_exn"))
       >>= fun worker ->
       Unresponsive_worker.run_exn
         worker ~f:Unresponsive_worker.functions.wait ~arg:sleep_for
       >>= fun () ->
       Core.Std.Printf.printf "unresponsive worker returned\n%!";
       return ()
    )

(* This call to [Parallel.start_app] must be top level *)
let () = Parallel.start_app command
