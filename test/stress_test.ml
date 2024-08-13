open Core
open Async

module Worker = struct
  module T = struct
    type 'worker functions = { ping : ('worker, unit, unit) Rpc_parallel.Function.t }

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
      let ping =
        C.create_rpc
          ~f:(fun ~worker_state:() ~conn_state:() -> return)
          ~bin_input:Unit.bin_t
          ~bin_output:Unit.bin_t
          ()
      ;;

      let functions = { ping }
      let init_worker_state () = Deferred.unit
      let init_connection_state ~connection:_ ~worker_state:_ = return
    end
  end

  include Rpc_parallel.Make (T)
end

let command =
  Command.async_spec
    ~summary:"Stress testing Rpc parallel"
    Command.Spec.(
      empty
      +> flag "-worker" (optional string) ~doc:" worker to run test on"
      +> flag
           "-dir"
           (optional_with_default "~" string)
           ~doc:" directory to copy executable to"
      +> flag
           "-num-workers"
           (optional_with_default 20 int)
           ~doc:" number of workers to spawn"
      +> flag "-num-loops" (optional_with_default 30 int) ~doc:" number of loops to test")
    (fun worker dir num_workers num_loops () ->
      let setup =
        match worker with
        | None -> return (Rpc_parallel.How_to_run.local, fun () -> return ())
        | Some w ->
          (match%map
             Rpc_parallel.Remote_executable.copy_to_host ~executable_dir:dir w
           with
           | Error e -> Error.raise e
           | Ok exec ->
             ( Rpc_parallel.How_to_run.remote exec
             , fun () -> Rpc_parallel.Remote_executable.delete exec >>| Or_error.ok_exn ))
      in
      let%bind executable, cleanup = setup in
      let rec loop remaining =
        if remaining = 0
        then return ()
        else (
          let start =
            Time_float.to_span_since_epoch (Time_float.now ()) |> Time_float.Span.to_sec
          in
          let%bind () =
            Deferred.all_unit
              (List.map (List.range 0 num_workers) ~f:(fun _i ->
                 let%bind conn =
                   Worker.spawn_exn
                     ~how:executable
                     ~redirect_stdout:`Dev_null
                     ~shutdown_on:Connection_closed
                     ~redirect_stderr:`Dev_null
                     ~on_failure:Error.raise
                     ~connection_state_init_arg:()
                     ()
                 in
                 Worker.Connection.run_exn conn ~f:Worker.functions.ping ~arg:()))
          in
          let end_ =
            Time_float.to_span_since_epoch (Time_float.now ()) |> Time_float.Span.to_sec
          in
          Core.Printf.printf "%f\n%!" (end_ -. start);
          loop (remaining - 1))
      in
      let%bind () = loop num_loops in
      cleanup ())
    ~behave_nicely_in_pipeline:false
;;

let () = Rpc_parallel_krb_public.start_app ~krb_mode:For_unit_test command
