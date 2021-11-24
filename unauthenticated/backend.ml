open! Core
open! Async

let name = "Unauthenticated Async RPC"

module Settings = struct
  type t = unit [@@deriving bin_io, sexp]
end

let serve
      ?max_message_size
      ?handshake_timeout
      ?heartbeat_config
      ~implementations
      ~initial_connection_state
      ~where_to_listen
      ()
  =
  Rpc.Connection.serve
    ?max_message_size
    ?handshake_timeout
    ?heartbeat_config
    ~implementations
    ~initial_connection_state
    ~where_to_listen
    ()
;;

let client
      ?implementations
      ?max_message_size
      ?handshake_timeout
      ?heartbeat_config
      ?description
      ()
      where_to_connect
  =
  Rpc.Connection.client
    ?implementations
    ?max_message_size
    ?handshake_timeout
    ?heartbeat_config
    ?description
    where_to_connect
  |> Deferred.Or_error.of_exn_result
;;

let with_client
      ?implementations
      ?max_message_size
      ?handshake_timeout
      ?heartbeat_config
      ()
      where_to_connect
      f
  =
  Rpc.Connection.with_client
    ?implementations
    ?max_message_size
    ?handshake_timeout
    ?heartbeat_config
    where_to_connect
    f
  |> Deferred.Or_error.of_exn_result
;;
