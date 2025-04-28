open! Core
open! Async

let name = "Kerberized Async RPC"

module Settings = struct
  type t = Mode.t [@@deriving bin_io, sexp]

  let test_principal = lazy (Krb.Principal.Name.User (Core_unix.getlogin ()))

  let server_mode = function
    | Mode.Kerberized kerberized -> Mode.Kerberized.krb_server_mode kerberized
    | For_unit_test ->
      return
        (Krb.Mode.Server.test_with_principal ~test_principal:(force test_principal) ())
  ;;

  let client_mode = function
    | Mode.Kerberized kerberized -> Mode.Kerberized.krb_client_mode kerberized
    | For_unit_test ->
      Krb.Mode.Client.test_with_principal ~test_principal:(force test_principal) ()
  ;;
end

let authorize_current_principal () =
  let%map principal_to_authorize =
    if am_running_test
    then
      (* There isn't a cred cache in the testing environment, so just use the current
         username. *)
      Unix.getlogin () >>| fun x -> Krb.Principal.Name.User x
    else
      Krb.Cred_cache.default_principal ()
      (* This will raise if there is the default credential cache doesn't exist. If this
         is the case, we'd expect [Krb.Rpc.Connection.serve] to have already failed. *)
      >>| Or_error.ok_exn
  in
  Krb.Authorize.accept_single principal_to_authorize
;;

let serve
  ?max_message_size
  ?buffer_age_limit
  ?handshake_timeout
  ?heartbeat_config
  ~implementations
  ~initial_connection_state
  ~where_to_listen
  settings
  =
  let%bind authorize = authorize_current_principal () in
  let%bind krb_mode = Settings.server_mode settings in
  Krb.Rpc.Connection.serve
    ~implementations
    ~initial_connection_state:(fun (_ : Krb.Client_identity.t) inet connection ->
      initial_connection_state inet connection)
    ~authorize
    ~krb_mode
    ?max_message_size
    ?buffer_age_limit
    ?handshake_timeout
    ?heartbeat_config
    ~where_to_listen
    ()
  |> Deferred.Or_error.ok_exn
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
  let%bind authorize = authorize_current_principal () in
  let krb_mode = Settings.client_mode settings in
  Krb.Rpc.Connection.with_client
    ?implementations:(Option.map ~f:Fn.const implementations)
    ?max_message_size
    ?buffer_age_limit
    ?handshake_timeout
    ?heartbeat_config
    ~krb_mode
    ~authorize
    where_to_connect
    f
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
  let%bind authorize = authorize_current_principal () in
  let krb_mode = Settings.client_mode settings in
  Krb.Rpc.Connection.client
    ?implementations:(Option.map ~f:Fn.const implementations)
    ?max_message_size
    ?buffer_age_limit
    ?handshake_timeout
    ?heartbeat_config
    ?description
    ~krb_mode
    ~authorize
    where_to_connect
;;
