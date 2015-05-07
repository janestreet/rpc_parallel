open Core.Std

module Id : Unique_id

type t = Id.t with bin_io

include Hashable.S_binable with type t := t
