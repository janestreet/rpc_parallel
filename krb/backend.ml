open! Core
open! Async

let name = "Kerberized Async RPC"

module Settings = struct
  type t = Mode.t [@@deriving bin_io, sexp]

  let test_principal = lazy (Krb_public.Principal.Name.User (Core_unix.getlogin ()))

  let server_mode = function
    | Mode.Kerberized kerberized -> Mode.Kerberized.krb_server_mode kerberized
    | For_unit_test ->
      return
        (Krb_public.Mode.Server.test_with_principal
           ~test_principal:(force test_principal)
           ())
  ;;

  let client_mode = function
    | Mode.Kerberized kerberized -> Mode.Kerberized.krb_client_mode kerberized
    | For_unit_test ->
      Krb_public.Mode.Client.test_with_principal ~test_principal:(force test_principal) ()
  ;;
end

(* We enforce that the master and worker must have matching kerberos principals *)
let authorize_current_principal () =
  Username_async.unix_effective ()
  >>| Username_async.to_string
  >>| (fun x -> Krb_public.Principal.Name.User x)
  >>| Krb_public.Authorize.accept_single
;;

let serve
      ?max_message_size
      ?handshake_timeout
      ?heartbeat_config
      ~implementations
      ~initial_connection_state
      ~where_to_listen
      settings
  =
  let%bind authorize = authorize_current_principal () in
  let%bind krb_mode = Settings.server_mode settings in
  Krb_public.Rpc.Connection.serve
    ~implementations
    ~initial_connection_state:(fun (_ : Krb_public.Client_identity.t) inet connection ->
      initial_connection_state inet connection)
    ~authorize
    ~krb_mode
    ?max_message_size
    ?handshake_timeout
    ?heartbeat_config
    ~where_to_listen
    ()
  |> Deferred.Or_error.ok_exn
;;

let with_client
      ?implementations
      ?max_message_size
      ?handshake_timeout
      ?heartbeat_config
      settings
      where_to_connect
      f
  =
  let%bind authorize = authorize_current_principal () in
  let krb_mode = Settings.client_mode settings in
  Krb_public.Rpc.Connection.with_client
    ?implementations:(Option.map ~f:Fn.const implementations)
    ?max_message_size
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
      ?handshake_timeout
      ?heartbeat_config
      ?description
      settings
      where_to_connect
  =
  let%bind authorize = authorize_current_principal () in
  let krb_mode = Settings.client_mode settings in
  Krb_public.Rpc.Connection.client
    ?implementations:(Option.map ~f:Fn.const implementations)
    ?max_message_size
    ?handshake_timeout
    ?heartbeat_config
    ?description
    ~krb_mode
    ~authorize
    where_to_connect
;;
