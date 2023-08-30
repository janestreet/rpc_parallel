open! Core
open! Async

type t =
  { max_message_size : int option [@sexp.option]
  ; buffer_age_limit : Writer.buffer_age_limit option [@sexp.option]
  ; handshake_timeout : Time_float.Span.t option [@sexp.option]
  ; heartbeat_config : Rpc.Connection.Heartbeat_config.t option [@sexp.option]
  }
[@@deriving sexp, bin_io]

let env_var = "RPC_PARALLEL_RPC_SETTINGS"

let default =
  { max_message_size = None
  ; buffer_age_limit = None
  ; handshake_timeout = None
  ; heartbeat_config = None
  }
;;

let to_string_for_env_var
  ?max_message_size
  ?buffer_age_limit
  ?handshake_timeout
  ?heartbeat_config
  ()
  =
  let t = { max_message_size; buffer_age_limit; handshake_timeout; heartbeat_config } in
  Sexp.to_string (sexp_of_t t)
;;

let%expect_test _ =
  let heartbeat_config =
    Rpc.Connection.Heartbeat_config.create
      ~timeout:Time_ns.Span.hour
      ~send_every:Time_ns.Span.minute
      ()
  in
  let () = print_string (to_string_for_env_var ()) in
  [%expect {| () |}];
  let () = print_string (to_string_for_env_var ~heartbeat_config ()) in
  [%expect {| ((heartbeat_config((timeout 1h)(send_every 1m)))) |}];
  return ()
;;

let create_with_env_override'
  ~env_var
  ~max_message_size
  ~buffer_age_limit
  ~handshake_timeout
  ~heartbeat_config
  =
  match Sys.getenv env_var with
  | None -> { max_message_size; buffer_age_limit; handshake_timeout; heartbeat_config }
  | Some value ->
    let from_env = [%of_sexp: t] (Sexp.of_string value) in
    { max_message_size = Option.first_some from_env.max_message_size max_message_size
    ; buffer_age_limit = Option.first_some from_env.buffer_age_limit buffer_age_limit
    ; handshake_timeout = Option.first_some from_env.handshake_timeout handshake_timeout
    ; heartbeat_config = Option.first_some from_env.heartbeat_config heartbeat_config
    }
;;

let create_with_env_override = create_with_env_override' ~env_var

module For_internal_testing = struct
  let create_with_env_override = create_with_env_override'
end
