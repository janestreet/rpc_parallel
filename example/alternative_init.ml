(* Note: this example uses deprecated initialization functions which are only available in
   the rpc_parallel_unauthenticated library, and not rpc_parallel_krb *)
open Core
open Async

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
      let functions = ()
      let init_worker_state () = Deferred.unit
      let init_connection_state ~connection:_ ~worker_state:_ = return
    end
  end

  include Rpc_parallel.Make (T)
end

let worker_command =
  let open Command.Let_syntax in
  Command.Staged.async
    ~summary:"for internal use"
    (let%map_open () = return () in
     fun () ->
       let worker_env = Rpc_parallel.Expert.worker_init_before_async_exn () in
       stage (fun `Scheduler_started ->
         Rpc_parallel_unauthenticated.Expert.start_worker_server_exn worker_env;
         Deferred.never ()))
    ~behave_nicely_in_pipeline:false
;;

let main_command =
  let open Command.Let_syntax in
  Command.async
    ~summary:"start the master and spawn a worker"
    (let%map_open () = return () in
     fun () ->
       let open Deferred.Let_syntax in
       Rpc_parallel_unauthenticated.Expert.start_master_server_exn
         ~worker_command_args:[ "worker" ]
         ();
       let%map (_connection : Worker.Connection.t) =
         Worker.spawn_exn
           ~on_failure:Error.raise
           ~shutdown_on:Connection_closed
           ~connection_state_init_arg:()
           ~redirect_stdout:`Dev_null
           ~redirect_stderr:`Dev_null
           ()
       in
       printf "Success.\n")
    ~behave_nicely_in_pipeline:false
;;

let command =
  Command.group
    ~summary:"Using Rpc_parallel.Expert"
    [ "worker", worker_command; "main", main_command ]
;;

let () = Command_unix.run command
