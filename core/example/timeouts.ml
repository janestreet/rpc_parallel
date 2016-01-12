open Core.Std
open Async.Std
open Rpc_parallel_core_deprecated.Std

(* A simple use of the [Rpc_parallel_core] library, testing the timeouts. Spawns a worker
   that is unresponsive for an extended period of time before replying. *)

(* Unresponsive Worker *)
let unresponsive_rpc =
  Rpc.Rpc.create
    ~name:"unresponsive_rpc"
    ~version:0
    ~bin_query:Int.bin_t
    ~bin_response:Unit.bin_t

let unresponsive_rpc_impl =
  Rpc.Rpc.implement unresponsive_rpc (fun () sleep_time ->
    Core.Std.Unix.sleep sleep_time;
    return ()
  )

let implementations =
  Rpc.Implementations.create_exn ~implementations:[unresponsive_rpc_impl]
    ~on_unknown_rpc:`Close_connection

let worker_main () =
  Rpc.Connection.serve ~implementations
    ~initial_connection_state:(fun _ _ -> ())
    ~where_to_listen:Tcp.on_port_chosen_by_os ()
  >>| fun serv ->
  Host_and_port.create ~host:(Unix.gethostname()) ~port:(Tcp.Server.listening_on serv)

module Parallel_app = Parallel_deprecated.Make(struct
  type worker_arg = unit [@@deriving bin_io]
  type worker_ret = Host_and_port.t [@@deriving bin_io]
  let worker_main = worker_main
end)

let handle_error worker err =
  failwiths (sprintf "error in %s" worker) err Error.sexp_of_t

let command =
  Command.async ~summary:"foo"
    Command.Spec.(
      empty
      +> flag "sleep-for" (required int) ~doc:""
      +> flag "timeout" (required int) ~doc:""
    )
    (fun sleep_for timeout_int () ->
       let heartbeat_config =
         let timeout = Time_ns.Span.of_sec (Float.of_int timeout_int) in
         let send_every = Time_ns.Span.of_sec (Float.of_int (timeout_int / 3)) in
         Rpc.Connection.Heartbeat_config.create ~timeout ~send_every
       in
       Parallel_app.spawn_worker_exn
         ~rpc_handshake_timeout:(Time.Span.of_sec (Float.of_int timeout_int))
         ~rpc_heartbeat_config:heartbeat_config
         ~where:`Local
         ~redirect_stdout:`Dev_null
         ~redirect_stderr:`Dev_null
         ()
         ~on_failure:(handle_error "unresponsive worker") >>= fun (worker, _id) ->
       (Rpc.Connection.with_client
          ~handshake_timeout:(Time.Span.of_sec (Float.of_int timeout_int))
          ~heartbeat_config
          ~host:(Host_and_port.host worker)
          ~port:(Host_and_port.port worker)
          (fun conn -> Rpc.Rpc.dispatch unresponsive_rpc conn sleep_for)
        >>| function
        | Error e -> failwiths "Connection failure" e Exn.sexp_of_t
        | Ok (Error e) -> failwiths "Dispatch failure" e Error.sexp_of_t
        | Ok (Ok i) -> i
       )
       >>= fun () ->
       Core.Std.Printf.printf "worker returned\n";
       return ())

let () = Parallel_app.run command
