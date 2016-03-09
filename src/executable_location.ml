open! Core.Std

type t =
  | Local
  | Remote : _ Remote_executable.t -> t
