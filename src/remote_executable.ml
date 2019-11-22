open Core
open Poly
open Async

type 'a t =
  { host : string
  ; path : string
  ; host_key_checking : string list
  }
[@@deriving fields]

let hostkey_checking_options opt =
  match opt with
  | None -> []
  (* Use ssh default *)
  | Some `Ask -> [ "-o"; "StrictHostKeyChecking=ask" ]
  | Some `No -> [ "-o"; "StrictHostKeyChecking=no" ]
  | Some `Yes -> [ "-o"; "StrictHostKeyChecking=yes" ]
;;

let existing_on_host ~executable_path ?strict_host_key_checking host =
  { host
  ; path = executable_path
  ; host_key_checking = hostkey_checking_options strict_host_key_checking
  }
;;

let copy_to_host ~executable_dir ?strict_host_key_checking host =
  let our_basename = Filename.basename Sys.executable_name in
  Process.run ~prog:"mktemp" ~args:[ "-u"; sprintf "%s.XXXXXXXX" our_basename ] ()
  >>=? fun new_basename ->
  let options = hostkey_checking_options strict_host_key_checking in
  let path = String.strip (executable_dir ^/ new_basename) in
  Process.run
    ~prog:"scp"
    ~args:(options @ [ Utils.our_binary (); sprintf "%s:%s" host path ])
    ()
  >>|? Fn.const { host; path; host_key_checking = options }
;;

let delete executable =
  Process.run
    ~prog:"ssh"
    ~args:(executable.host_key_checking @ [ executable.host; "rm"; executable.path ])
    ()
  >>|? Fn.const ()
;;

let env_for_ssh env =
  let cheesy_escape str = Sexp.to_string (String.sexp_of_t str) in
  List.map env ~f:(fun (key, data) -> key ^ "=" ^ cheesy_escape data)
;;

let run exec ~env ~args ~wrap =
  Utils.our_md5 ()
  >>=? fun md5 ->
  Process.run
    ~prog:"ssh"
    ~args:(exec.host_key_checking @ [ exec.host; "md5sum"; exec.path ])
    ()
  >>=? fun remote_md5 ->
  let remote_md5, _ = String.lsplit2_exn ~on:' ' remote_md5 in
  if md5 <> remote_md5
  then
    Deferred.Or_error.errorf
      "The remote executable %s:%s does not match the local executable"
      exec.host
      exec.path
  else (
    let { Prog_and_args.prog; args } = wrap { Prog_and_args.prog = exec.path; args } in
    Process.create
      ~prog:"ssh"
      ~args:(exec.host_key_checking @ [ exec.host ] @ env_for_ssh env @ [ prog ] @ args)
      ())
;;
