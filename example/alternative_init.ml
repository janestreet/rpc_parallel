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

    module Functions
        (C : Parallel.Creator
         with type worker_state := Worker_state.t
          and type connection_state := Connection_state.t) = struct
      let functions = ()

      let init_worker_state ~parent_heartbeater () =
        Parallel.Heartbeater.(if_spawned connect_and_shutdown_on_disconnect_exn)
          parent_heartbeater
        >>| fun ( `Connected | `No_parent ) -> ()

      let init_connection_state ~connection:_ ~worker_state:_ = return
    end
  end
  include Parallel.Make(T)
end

let worker_command =
  let open Command.Let_syntax in
  Command.basic'
    ~summary:"for internal use"
    [%map_open
      let () = return ()
      in
      fun () -> never_returns (Parallel.Expert.run_as_worker_exn ())
    ]

let main_command =
  let open Command.Let_syntax in
  Command.async'
    ~summary:"start the master and spawn a worker"
    [%map_open
      let () = return ()
      in
      fun () ->
        Parallel.Expert.init_master_exn ~worker_command_args:["worker"] ();
        Worker.spawn_and_connect_exn ~on_failure:Error.raise
          ~redirect_stdout:`Dev_null ~redirect_stderr:`Dev_null
          ~connection_state_init_arg:() ()
        >>| fun (_worker, _connection) ->
        printf "Success.\n"
    ]

let command =
  Command.group
    ~summary:"Simple use of Async Parallel V2"
    [ "worker", worker_command
    ; "main", main_command
    ]

let () = Command.run command
