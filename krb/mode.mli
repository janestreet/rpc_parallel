open! Core
open Async

module Kerberized : sig
  type t

  val krb_server_mode : t -> Krb.Mode.Server.t Deferred.t
  val krb_client_mode : t -> Krb.Mode.Client.t
end

type t =
  | Kerberized of Kerberized.t
  | For_unit_test
[@@deriving bin_io, sexp]

(** In this mode, communication to and from workers will use Kerberos for authentication.

    [key_source] must be a valid key source for all the hosts where a worker is running.
    For example, if your key source specifies a keytab path, that path must have a valid
    keytab on hosts where you spawn a worker.

    [conn_type] specifies the level of protection for the rpc communication to and from
    workers. If your application is extremely performance sensitive and sends a lot of
    data to and from your workers, you should use [Auth]. Otherwise, using [Priv] as a
    default is appropriate. See lib/krb/public/src/conn_type.mli for more documentation.

    Regardless of what [conn_type] you choose, connections will only be accepted from
    principals that match the principal associated with [key_source]. By default, this is
    the principal associated with the user that started the rpc_parallel process. *)
val kerberized
  :  key_source:Krb.Server_key_source.t
  -> conn_type:Krb.Conn_type.t
  -> unit
  -> t

(** In this mode, communication to and from workers will not be kerberized. This is only
    appropriate if you are running a test. *)
val for_unit_test : t
