open! Core
open Async

module Kerberized = struct
  type t =
    { conn_type : Krb_public.Conn_type.Stable.V1.t
    ; server_key_source : Krb_public.Server_key_source.Stable.V2.t
    }
  [@@deriving bin_io, sexp]

  let krb_server_mode t =
    let conn_type_preference = Krb_public.Conn_type_preference.accept_only t.conn_type in
    let key_source = t.server_key_source in
    Krb_public.Mode.Server.kerberized ~conn_type_preference ~key_source |> return
  ;;

  let krb_client_mode t =
    let conn_type_preference = Krb_public.Conn_type_preference.accept_only t.conn_type in
    Krb_public.Mode.Client.kerberized ~conn_type_preference ()
  ;;
end

type t =
  | Kerberized of Kerberized.t
  | For_unit_test
[@@deriving bin_io, sexp]

let kerberized ~key_source ~conn_type () =
  Kerberized { conn_type; server_key_source = key_source }
;;

let for_unit_test = For_unit_test
