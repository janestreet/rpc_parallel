open Core
open Async

(* When this binary is launched as an [Rpc_parallel] worker, it intentionally writes a
   couple of lines to stderr and exits non-zero before daemonization completes. This
   simulates a pre-daemonization failure (such as a Krb handshake error) and exercises
   [wait_for_daemonization_and_collect_stderr] in the master so we can confirm how
   captured stderr is forwarded to the master. *)
let () =
  match Rpc_parallel.Utils.whoami () with
  | `Worker ->
    let stderr = Out_channel.stderr in
    Out_channel.output_string stderr "RP_TEST_STDERR_LINE_1\n";
    Out_channel.output_string stderr "RP_TEST_STDERR_LINE_2\n";
    Out_channel.flush stderr;
    Stdlib.exit 1
  | `Master -> ()
;;

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

    module Functions
        (_ : Rpc_parallel.Creator
             with type worker_state := Worker_state.t
              and type connection_state := Connection_state.t) =
    struct
      let init_worker_state () = Deferred.unit
      let init_connection_state ~connection:_ ~worker_state:_ () = return ()
      let functions = ()
    end
  end

  include Rpc_parallel.Make (T)
end

let main ~log_file =
  Log.Global.set_output [ Log.Output.file `Text ~filename:log_file ];
  match%bind
    Worker.spawn
      ~on_failure:Error.raise
      ~shutdown_on:Heartbeater_connection_timeout
      ~redirect_stdout:`Dev_null
      ~redirect_stderr:`Dev_null
      ()
  with
  | Ok (_ : Worker.t) -> failwith "expected spawn to fail but it did not"
  | Error error ->
    let%bind () = Log.Global.flushed () in
    [%log.info "Worker.spawn returned error" (error : Error.t)];
    return ()
;;

let command =
  Command.async
    ~summary:"Force a worker to exit before daemonization to exercise stderr capture."
    (let%map_open.Command log_file =
       flag "-log-file" (required string) ~doc:"PATH route Log.Global output to PATH"
     in
     fun () -> main ~log_file)
    ~behave_nicely_in_pipeline:false
;;

let () = Rpc_parallel_krb_public.start_app ~krb_mode:For_unit_test command
