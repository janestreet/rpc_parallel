open Core
open Async

module Shard = struct
  module T = struct
    type 'worker functions =
      ( 'worker
        , unit
          * (Float.t Rpc.Pipe_rpc.Direct_stream_writer.t -> unit Or_error.t Deferred.t)
        , unit )
        Rpc_parallel.Function.t

    module Worker_state = struct
      type t = unit
      type init_arg = unit [@@deriving bin_io]
    end

    module Connection_state = struct
      type t = unit
      type init_arg = unit [@@deriving bin_io]
    end

    module Functions
        (Creator : Rpc_parallel.Creator
                   with type worker_state = Worker_state.t
                    and type connection_state = Connection_state.t) =
    struct
      let do_work_for span = Core_unix.sleep (Float.to_int (Time_ns.Span.to_sec span))

      let functions =
        Creator.create_reverse_direct_pipe
          ~bin_query:Unit.bin_t
          ~bin_update:Float.bin_t
          ~bin_response:Unit.bin_t
          ~f:(fun ~worker_state:() ~conn_state:() () pipe ->
            Pipe.iter_without_pushback pipe ~f:(fun update ->
              ignore update;
              do_work_for (Time_ns.Span.of_min 3.)))
          ()
      ;;

      let init_worker_state () = Deferred.unit
      let init_connection_state ~connection:_ ~worker_state:_ () = Deferred.unit
    end
  end

  include T
  include Rpc_parallel.Make (T)
end

let main () =
  let%bind connection =
    Shard.spawn_exn
      ~shutdown_on:Connection_closed
      ~redirect_stdout:`Dev_null
      ~redirect_stderr:`Dev_null
      ~on_failure:Error.raise
      ~connection_state_init_arg:()
      ()
  in
  let () =
    let f direct_writer =
      Clock.every (Time_float.Span.of_sec 1.) (fun () ->
        for _ = 0 to 1_000_000 do
          match
            Rpc.Pipe_rpc.Direct_stream_writer.write_without_pushback
              direct_writer
              (Random.float 100.)
          with
          | `Closed -> ()
          | `Ok -> ()
        done);
      Ok () |> return
    in
    Shard.Connection.run_exn connection ~f:Shard.functions ~arg:((), f) |> don't_wait_for
  in
  Deferred.never ()
;;

let readme () =
  {|
Running the command without setting any environment variables causes the following error
after about 2 minutes:

$ ./buffer_age_limit
(monitor.ml.Error
 ((rpc_error
   (Uncaught_exn
    (monitor.ml.Error
     ("writer buffer has data older than" (maximum_age 2m)
     ...

If you set the RPC_PARALLEL_RPC_SETTINGS environment variable to "((buffer_age_limit Unlimited))"
then after 10 minutes you get the following error:

$ RPC_PARALLEL_RPC_SETTINGS="((buffer_age_limit Unlimited))" ./buffer_age_limit.exe
(monitor.ml.Error
 ((rpc_error (Connection_closed ("No heartbeats received for 10m.")))
  (connection_description
   ("Kerberized RPC client" (connected_to (<omitted> 39075))
    (client_principal (User <omitted>)) (server_principal (User <omitted>))))
  (rpc_name rpc_parallel_reverse_piped_0) (rpc_version 0))
 ("Called from Base__Or_error.ok_exn in file \"or_error.ml\", line 118, characters 17-32"))
|}
;;

let command =
  Command.async
    ~summary:"test"
    ~readme
    (let%map_open.Command () = return () in
     fun () -> main ())
;;

let () =
  Rpc_parallel_krb_public.start_app
    ~krb_mode:For_unit_test
    ~rpc_heartbeat_config:
      (Rpc.Connection.Heartbeat_config.create ~timeout:(Time_ns.Span.of_min 10.) ())
    command
;;
