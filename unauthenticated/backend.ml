open! Core
open! Async

let name = "Unauthenticated Async RPC"

module Settings = struct
  type t =
    | Only_local
    | Allow_remote
  [@@deriving bin_io, sexp]

  let only_allow_local_workers_will_fail_to_connect_if_remote = Only_local
  let unsafe_allow_unauthenticated_remote_workers = Allow_remote
end

let serve
  ?max_message_size
  ?buffer_age_limit
  ?handshake_timeout
  ?heartbeat_config
  ~implementations
  ~initial_connection_state
  ~where_to_listen
  (settings : Settings.t)
  =
  let where_to_listen =
    match settings with
    | Allow_remote -> where_to_listen
    | Only_local ->
      let port =
        Tcp.Where_to_listen.address where_to_listen |> Socket.Address.Inet.port
      in
      Tcp.Where_to_listen.bind_to Localhost (On_port port)
  in
  let make_transport fd ~max_message_size =
    Rpc.Transport.of_fd ?buffer_age_limit fd ~max_message_size
  in
  Rpc.Connection.serve
    ?max_message_size
    ~make_transport
    ?handshake_timeout
    ?heartbeat_config
    ~implementations
    ~initial_connection_state
    ~where_to_listen
    ()
;;

(* This is necessary binding to [localhost] requires also specifying [localhost] when
   connecting. Specifying the current box's hostname or ip doesn't work *)
let handle_where_to_connect where_to_connect (settings : Settings.t) =
  match settings with
  | Allow_remote -> return where_to_connect
  | Only_local ->
    let%map remote_address = Tcp.Where_to_connect.remote_address where_to_connect in
    let remote_port = Socket.Address.Inet.port remote_address in
    Tcp.Where_to_connect.of_inet_address
      (Socket.Address.Inet.create Unix.Inet_addr.localhost ~port:remote_port)
;;

let client
  ?implementations
  ?max_message_size
  ?buffer_age_limit
  ?handshake_timeout
  ?heartbeat_config
  ?description
  settings
  where_to_connect
  =
  let%bind where_to_connect = handle_where_to_connect where_to_connect settings in
  let make_transport fd ~max_message_size =
    Rpc.Transport.of_fd ?buffer_age_limit fd ~max_message_size
  in
  Rpc.Connection.client
    ?implementations
    ?max_message_size
    ~make_transport
    ?handshake_timeout
    ?heartbeat_config
    ?description
    where_to_connect
  |> Deferred.Or_error.of_exn_result
;;

let with_client
  ?implementations
  ?max_message_size
  ?buffer_age_limit
  ?handshake_timeout
  ?heartbeat_config
  settings
  where_to_connect
  f
  =
  let%bind where_to_connect = handle_where_to_connect where_to_connect settings in
  let make_transport fd ~max_message_size =
    Rpc.Transport.of_fd ?buffer_age_limit fd ~max_message_size
  in
  Rpc.Connection.with_client
    ?implementations
    ?max_message_size
    ~make_transport
    ?handshake_timeout
    ?heartbeat_config
    where_to_connect
    f
  |> Deferred.Or_error.of_exn_result
;;
