open! Core

type t =
  { prog : string
  ; args : string list
  }
[@@deriving sexp_of]
