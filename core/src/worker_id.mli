open! Core.Std

type t [@@deriving bin_io]

val create : unit -> t

include Hashable.S_binable with type t := t
include Stringable.S       with type t := t
