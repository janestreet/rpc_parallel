(** See lib/rpc_parallel/src/parallel_intf.ml for documentation. *)

open Core
open Async

(** See lib/rpc_parallel/src/parallel_intf.ml for documentation. Backend_and_settings.t is
    specialized to expose the krb server and client modes. *)
val start_app
  :  ?rpc_max_message_size:int
  -> ?rpc_buffer_age_limit:Writer.buffer_age_limit
  -> ?rpc_handshake_timeout:Time_float.Span.t
  -> ?rpc_heartbeat_config:Rpc.Connection.Heartbeat_config.t
  -> ?when_parsing_succeeds:(unit -> unit)
  -> krb_mode:Mode.t
  -> Command.t
  -> unit

module For_testing : sig
  (** See lib/rpc_parallel/src/parallel_intf.ml for documentation. The krb server and
      client modes are set for testing so that manually configuring a kerberos sandbox in
      your jbuild is unnecessary. *)
  val initialize : Source_code_position.t -> unit
end

module Expert : sig
  (** See lib/rpc_parallel/src/parallel_intf.ml for documentation. Backend_and_settings.t
      is specialized to expose the krb server and client modes. *)
  val start_master_server_exn
    :  ?rpc_max_message_size:int
    -> ?rpc_buffer_age_limit:Writer.buffer_age_limit
    -> ?rpc_handshake_timeout:Time_float.Span.t
    -> ?rpc_heartbeat_config:Rpc.Connection.Heartbeat_config.t
    -> ?pass_name:bool (** default: true *)
    -> krb_mode:Mode.t
    -> worker_command_args:string list
    -> unit
    -> unit

  (** See lib/rpc_parallel/src/parallel_intf.ml for documentation. *)
  val worker_command : Command.t
end
