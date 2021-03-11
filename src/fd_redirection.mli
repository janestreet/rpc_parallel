open! Core

(** This variant determines the where the file descriptor is redirect towards.

    In the [`Dev_null] case it is redirected to /dev/null, ie is ignored.
    
    In both [`File_append location] and [`File_truncate location] it is redirected to [location], where [location] is the absolute path to the file. 
*)
type t =
  [ `Dev_null
  | `File_append of string
  | `File_truncate of string
  ]
[@@deriving sexp]
