open Core.Std
open Async.Std
open Rpc_parallel.Std

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

    module Functions (C : Parallel.Creator) = struct
      let functions = ()

      let init_worker_state ~parent_heartbeater () =
        Parallel.Heartbeater.(if_spawned connect_and_shutdown_on_disconnect_exn)
          parent_heartbeater
        >>| fun ( `Connected | `No_parent ) ->
        (Clock.after (sec 1.) >>> fun () -> Log.Global.info "tick1");
        (Clock.after (sec 2.) >>> fun () -> Log.Global.info "tick2")

      let init_connection_state ~connection:_ ~worker_state:_ = return
    end
  end
  include Parallel.Make(T)
end

let main () =
  Worker.spawn ~on_failure:Error.raise
    ~redirect_stdout:`Dev_null ~redirect_stderr:`Dev_null ()
  >>=? fun worker ->
  Worker.Connection.client worker ()
  >>=? fun conn ->
  Worker.Connection.run conn ~f:Parallel.Function.async_log ~arg:()
  >>=? fun log ->
  Worker.Connection.run conn ~f:Parallel.Function.async_log ~arg:()
  >>=? fun log2 ->
  don't_wait_for(Pipe.iter log ~f:(fun line ->
    printf !"1: %{sexp:Log.Message.Stable.V2.t}\n" line |> return));
  don't_wait_for(Pipe.iter log2 ~f:(fun line ->
    printf !"2: %{sexp:Log.Message.Stable.V2.t}\n" line |> return));
  Clock.after (sec 1.5)
  >>= fun () ->
  Pipe.close_read log;
  Clock.after (sec 1.)
  >>| fun () ->
  Ok ()

let command =
  Command.async_or_error ~summary:"Using the built in log redirection function"
    Command.Spec.empty
    main

let () = Parallel.start_app command
