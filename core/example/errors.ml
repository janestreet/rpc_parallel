open Core.Std
open Async.Std
open Rpc_parallel_core_deprecated.Std

type arg = [`Ok | `Exn_now | `Exn_later] [@@deriving bin_io]

let exn_later_delay = sec 1.
let got_exn_later = ref false

let worker_main arg =
  match arg with
  | `Ok -> return ()
  | `Exn_now -> failwith "Exn_now"
  | `Exn_later ->
    (Clock.after exn_later_delay
     >>> fun () -> failwith "Exn_later");
    return ()

module Parallel_app = Parallel_deprecated.Make(struct
  type worker_arg = arg [@@deriving bin_io]
  type worker_ret = unit [@@deriving bin_io]
  let worker_main = worker_main
end)

let on_failure ~expect e =
  if expect then begin
    printf !"Expected to fail. Got %{sexp:Error.t}\n" e;
    got_exn_later := true
  end else
    failwiths "expected no failure" e Error.sexp_of_t

let command =
  Command.async ~summary:"foo"
    Command.Spec.(
      empty
      +> flag "log-dir" (required string)
           ~doc:" Folder to write worker logs to"
    )
    (fun cd () ->
       let redirect_stdout, redirect_stderr =
         `File_truncate "worker.out", `File_truncate "worker.err"
       in
       Parallel_app.spawn_worker `Ok ~cd ~redirect_stdout ~redirect_stderr
         ~on_failure:(on_failure ~expect:false)
       >>= function
       | Error e ->
         failwiths "expected to succeed" e Error.sexp_of_t
       | Ok ((), _id1) ->
         Parallel_app.spawn_worker `Exn_now ~cd ~redirect_stdout ~redirect_stderr
           ~on_failure:(on_failure ~expect:false)
         >>= function
         | Ok ((), _id2) -> failwith "expected to fail"
         | Error e ->
           printf !"Expected to fail with worker exception. Got %{sexp:Error.t}\n" e;
           Parallel_app.spawn_worker `Exn_later ~cd ~redirect_stdout ~redirect_stderr
             ~on_failure:(on_failure ~expect:true)
           >>= function
           | Error e ->
             failwiths "expected to succeed" e Error.sexp_of_t
           | Ok ((), _id3) ->
             Clock.after (Time.Span.(+) (sec 1.) exn_later_delay)
             >>| fun () ->
             assert !got_exn_later)

let () = Parallel_app.run command
