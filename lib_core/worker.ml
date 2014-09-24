open Core.Std

module Id = Unique_id.Int(Unit)

include Hashable.Make_binable (struct
  type t = Id.t with bin_io, sexp
  let compare = compare
  let hash = Hashtbl.hash
end)

type t = Id.t with bin_io
