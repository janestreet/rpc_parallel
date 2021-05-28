open! Core

(** This variant specifies how to redirect the file descriptor.

    In the [`Dev_null] case, it is redirected to /dev/null (i.e. the output is ignored).

    In either [`File_append location] or [`File_truncate location], it is redirected to
    [location], where [location] is the absolute path to the file. [`File_append] will
    append to the file, while [`File_truncate] will fully overwrite it. *)
type t =
  [ `Dev_null
  | `File_append of string
  | `File_truncate of string
  ]
[@@deriving sexp]
