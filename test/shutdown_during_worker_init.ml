open Core
open Async

module Slow_worker = struct
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
      let init_worker_state () = after (sec 60.)
      let init_connection_state ~connection:_ ~worker_state:_ = return
    end
  end

  include Rpc_parallel.Make (T)
end

let readme () =
  {|
This is a manual rpc-parallel test. You should kill the master process a couple seconds after
starting it and then see whether or not the child immediately shuts itself down or not.
|}
;;

let command =
  Command.async
    ~summary:"Exercise a worker who is in the middle of init when the master shutsdown"
    ~readme
    (let%map_open.Command shutdown_on =
       choose_one
         ~if_nothing_chosen:Raise
         [ flag
             "-shutdown-on-heartbeater-connection-timeout"
             (no_arg_some `Heartbeater_connection_timeout)
             ~doc:"supply ~shutdown_on:Heartbeater_connection_timeout during spawn"
         ; flag
             "-shutdown-on-connection-closed"
             (no_arg_some `Connection_closed)
             ~doc:"supply ~shutdown_on:Connection_closed during spawn"
         ; flag
             "-shutdown-on-called-shutdown-function"
             (no_arg_some `Called_shutdown_function)
             ~doc:"supply ~shutdown_on:Called_shutdown_function during spawn"
         ]
     in
     fun () ->
       match shutdown_on with
       | `Heartbeater_connection_timeout ->
         let%bind (_ : Slow_worker.t) =
           Slow_worker.spawn_exn
             ~shutdown_on:Heartbeater_connection_timeout
             ~redirect_stdout:`Dev_null
             ~redirect_stderr:`Dev_null
             ~on_failure:(fun e -> Error.raise (Error.tag e ~tag:"spawn_exn"))
             ()
         in
         raise_s [%message "Worker spawn finished"]
       | `Connection_closed ->
         let%bind (_ : Slow_worker.Connection.t) =
           Slow_worker.spawn_exn
             ~shutdown_on:Connection_closed
             ~redirect_stdout:`Dev_null
             ~redirect_stderr:`Dev_null
             ~on_failure:(fun e -> Error.raise (Error.tag e ~tag:"spawn_exn"))
             ~connection_state_init_arg:()
             ()
         in
         raise_s [%message "Worker spawn finished"]
       | `Called_shutdown_function ->
         let%bind (_ : Slow_worker.t) =
           Slow_worker.spawn_exn
             ~shutdown_on:Called_shutdown_function
             ~redirect_stdout:`Dev_null
             ~redirect_stderr:`Dev_null
             ~on_failure:(fun e -> Error.raise (Error.tag e ~tag:"spawn_exn"))
             ()
         in
         raise_s [%message "Worker spawn finished"])
;;

let () = Rpc_parallel_krb_public.start_app ~krb_mode:For_unit_test command
