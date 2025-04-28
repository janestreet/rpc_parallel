open! Core

(** A specification for where to write outputs. *)
type t =
  [ `Dev_null (** Do not save the output anywhere. *)
  | `File_append of string
    (** Absolute path of a file to write to, creating the file if it does not already
        exist. *)
  | `File_truncate of string
    (** Absolute path of a file to write to, overwriting any existing file. *)
  ]
[@@deriving sexp]
