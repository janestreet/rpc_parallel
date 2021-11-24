open Core

module Settings : sig
  type t = Mode.t [@@deriving bin_io, sexp]
end

include Rpc_parallel.Backend with module Settings := Settings
