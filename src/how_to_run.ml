open! Core
open Async

type t =
  env:(string * string) list
  -> worker_command_args:string list
  -> wrap:(Prog_and_args.t -> Prog_and_args.t)
  -> Process.t Or_error.t Deferred.t

let local ~env ~worker_command_args ~wrap =
  let { Prog_and_args.prog; args } =
    wrap { Prog_and_args.prog = Current_exe.get_path (); args = worker_command_args }
  in
  Process.create ~prog ~argv0:(Sys.get_argv ()).(0) ~args ~env:(`Extend env) ()
;;

let remote ?assert_binary_hash exec ~env ~worker_command_args ~wrap =
  Remote_executable.run ?assert_binary_hash exec ~env ~args:worker_command_args ~wrap
;;

let wrap t ~f ~env ~worker_command_args ~wrap =
  t ~env ~worker_command_args ~wrap:(fun prog_and_args -> f (wrap prog_and_args))
;;

let run t ~env ~worker_command_args = t ~env ~worker_command_args ~wrap:Fn.id
