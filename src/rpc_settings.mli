open! Core
open! Async

type t =
  { max_message_size : int option
  ; buffer_age_limit : Writer.buffer_age_limit option
  ; handshake_timeout : Time_float.Span.t option
  ; heartbeat_config : Rpc.Connection.Heartbeat_config.t option
  }
[@@deriving bin_io, sexp]

(** [env_var] is the name of the environment variable read by rpc-parallel on start-up to
    inject additional rpc-settings for the application. *)
val env_var : string

(** Use all the default rpc settings. This is the record with [None] in every field. *)
val default : t

(** [to_string_for_env_var] generates the expected string format from the arguments
    matching the [start_app] function to be used with the [env_var] above. *)
val to_string_for_env_var
  :  ?max_message_size:int
  -> ?buffer_age_limit:Writer.buffer_age_limit
  -> ?handshake_timeout:Time_float.Span.t
  -> ?heartbeat_config:Rpc.Connection.Heartbeat_config.t
  -> unit
  -> string

val create_with_env_override
  :  max_message_size:int option
  -> buffer_age_limit:Writer.buffer_age_limit option
  -> handshake_timeout:Time_float.Span.t option
  -> heartbeat_config:Rpc.Connection.Heartbeat_config.t option
  -> t

module For_internal_testing : sig
  val create_with_env_override
    :  env_var:string
    -> max_message_size:int option
    -> buffer_age_limit:Writer.buffer_age_limit option
    -> handshake_timeout:Time_float.Span.t option
    -> heartbeat_config:Rpc.Connection.Heartbeat_config.t option
    -> t
end
