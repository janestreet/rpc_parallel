open! Core

module Settings : sig
  type t [@@deriving bin_io, sexp]

  val only_allow_local_workers_will_fail_to_connect_if_remote : t

  val unsafe_allow_unauthenticated_remote_workers : t
  [@@alert
    rpc_parallel_unauth_remote
      "Use [only_allow_local_workers_will_fail_to_connect_if_remote] unless you have \
       made sure this is safe"]
end

include Rpc_parallel.Backend with module Settings := Settings
