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
          ~f:(fun ~worker_state:() ~conn_state:() () -> return ())
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

let main ~host ~wrapper =
  let open Deferred.Or_error.Let_syntax in
  let executable_dir = Filename.temp_dir_name in
  let%bind remote_exec =
    Rpc_parallel.Remote_executable.copy_to_host
      ~strict_host_key_checking:`No
      ~executable_dir
      host
  in
  let how =
    Rpc_parallel.How_to_run.remote remote_exec
    |> Rpc_parallel.How_to_run.wrap ~f:(fun { prog; args } ->
      { prog = wrapper; args = prog :: args })
  in
  let%bind conn, process =
    Worker.spawn_in_foreground
      ~how
      ~shutdown_on:Connection_closed
      ~connection_state_init_arg:()
      ~on_failure:Error.raise
      ()
  in
  let%bind () = Worker.Connection.run conn ~f:Worker.functions.ping ~arg:() in
  print_endline "Worker successfully started";
  let%bind () = Deferred.ok (Worker.Connection.close conn) in
  let%bind (_ : Unix.Exit_or_signal.t) = Deferred.ok (Process.wait process) in
  let%bind () = Rpc_parallel.Remote_executable.delete remote_exec in
  let worker_stdout = Reader.lines (Process.stdout process) in
  Pipe.iter_without_pushback worker_stdout ~f:(fun line ->
    let line' = sprintf "[WORKER STDOUT]: %s\n" line in
    Writer.write (Lazy.force Writer.stdout) line')
  |> Deferred.ok
;;

let () =
  Command.async_or_error
    ~summary:"test using custom run exec_policy"
    (let%map_open.Command host = flag "host" (required string) ~doc:" host to connect to"
     and wrapper = flag "wrapper" (required string) ~doc:" wrapper command" in
     fun () -> main ~host ~wrapper)
    ~behave_nicely_in_pipeline:false
  |> Rpc_parallel_krb_public.start_app ~krb_mode:For_unit_test
;;
