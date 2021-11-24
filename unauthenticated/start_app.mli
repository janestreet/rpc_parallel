(** See lib/rpc_parallel/src/parallel_intf.ml for documentation. *)
open Core

open Async

(** See lib/rpc_parallel/src/parallel_intf.ml for documentation. *)
val start_app
  :  ?rpc_max_message_size:int
  -> ?rpc_handshake_timeout:Time.Span.t
  -> ?rpc_heartbeat_config:Rpc.Connection.Heartbeat_config.t
  -> ?when_parsing_succeeds:(unit -> unit)
  -> Command.t
  -> unit

module For_testing : sig
  (** See lib/rpc_parallel/src/parallel_intf.ml for documentation. *)
  val initialize : Source_code_position.t -> unit
end

module Expert : sig
  (** See lib/rpc_parallel/src/parallel_intf.ml for documentation. *)
  val start_master_server_exn
    :  ?rpc_max_message_size:int
    -> ?rpc_handshake_timeout:Time.Span.t
    -> ?rpc_heartbeat_config:Rpc.Connection.Heartbeat_config.t
    -> ?pass_name:bool (** default: true *)
    -> worker_command_args:string list
    -> unit
    -> unit

  (** See lib/rpc_parallel/src/parallel_intf.ml for documentation. *)
  val worker_command : Command.t

  (** See lib/rpc_parallel/src/parallel_intf.ml for documentation. *)
  val start_worker_server_exn : Rpc_parallel.Expert.Worker_env.t -> unit
end
