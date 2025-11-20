open Core
open Async

module Spec = struct
  type 'worker functions = { ping : ('worker, unit, unit) Rpc_parallel.Function.t }

  module Worker_state = struct
    type init_arg = unit [@@deriving bin_io]
    type t = unit
  end

  module Connection_state = struct
    type init_arg = unit [@@deriving bin_io]
    type t = unit
  end

  module Functions
      (C : Rpc_parallel.Creator
           with type worker_state := Worker_state.t
            and type connection_state := Connection_state.t) =
  struct
    let ping =
      C.create_rpc
        ~f:(fun ~worker_state:() ~conn_state:() -> return)
        ~bin_input:Unit.bin_t
        ~bin_output:Unit.bin_t
        ()
    ;;

    let functions = { ping }
    let init_worker_state () = Deferred.unit
    let init_connection_state ~connection:_ ~worker_state:_ = return
  end
end

module Worker = Rpc_parallel.Make (Spec)
module Worker_managed = Rpc_parallel.Managed.Make [@alert "-legacy"] (Spec)

let assert_fds here ~listen ~established =
  let here =
    sprintf
      "%s:%d:%d"
      here.Lexing.pos_fname
      here.Lexing.pos_lnum
      (here.Lexing.pos_cnum - here.Lexing.pos_bol)
  in
  let pid = Unix.getpid () in
  (* [lsof] has a return code of 1 for all errors (including finding no matching file
     descriptors for the query). Unfortunately we never quite know when [lsof] might
     return no file descriptors (see comment below), so we have to allow an exit code of
     1. *)
  let accept_nonzero_exit = [ 1 ] in
  (* There is a bug somewhere (lsof? linux kernel?) that causes lsof to be
     nondeterministic. I believe it has something to do with atomicity of /proc/net. we
     retry multiple times. Once in a while lsof will drop a tcp connection (and display
     "can't identify protocol"), then it will reappear on the next invocation. I have
     never seen it fail twice in a row. *)
  let rec try_lsof retries =
    match%bind
      Process.run_lines
        ~accept_nonzero_exit
        ~prog:"/usr/bin/lsof"
        ~args:[ "-ap"; Pid.to_string pid; "-iTCP" ]
        ()
    with
    | Error e -> failwiths "Unable to lsof" e [%sexp_of: Error.t]
    | Ok lines ->
      let filter to_match =
        List.filter lines ~f:(fun line ->
          Option.is_some (String.substr_index line ~pattern:to_match))
      in
      let cur_listen = List.length (filter "(LISTEN)") in
      let cur_established = List.length (filter "(ESTABLISHED)") in
      (match
         [%test_result: string * int] (here, cur_listen) ~expect:(here, listen);
         [%test_result: string * int] (here, cur_established) ~expect:(here, established)
       with
       | () -> Deferred.unit
       | exception e -> if retries > 0 then try_lsof (retries - 1) else raise e)
  in
  try_lsof 10
;;

let test_unmanaged () =
  let%bind worker =
    Worker.spawn_exn
      ~shutdown_on:Heartbeater_connection_timeout
      ~redirect_stderr:`Dev_null
      ~redirect_stdout:`Dev_null
      ~on_failure:Error.raise
      ()
  in
  (* The only connection we have is from
     [Rpc_parallel.Heartbeater.connect_and_shutdown_on_disconnect_exn]. And now finally
     the master rpc server has started. *)
  let%bind () = assert_fds [%here] ~listen:1 ~established:1 in
  let%bind connection = Worker.Connection.client_exn worker () in
  (* Now we have another connection *)
  let%bind () = assert_fds [%here] ~listen:1 ~established:2 in
  let%bind () = Worker.Connection.run_exn connection ~f:Worker.functions.ping ~arg:() in
  (* Still just these connections *)
  let%bind () = assert_fds [%here] ~listen:1 ~established:2 in
  let%bind () = Worker.Connection.close connection in
  let%bind () = Worker.Connection.close_finished connection in
  (* Now just the [Rpc_parallel.Heartbeater] connection *)
  let%bind () = assert_fds [%here] ~listen:1 ~established:1 in
  let%bind () = Worker.shutdown worker >>| ok_exn in
  (* Now no established connections *)
  assert_fds [%here] ~listen:1 ~established:0
;;

let test_serve () =
  let%bind worker = Worker.serve () in
  (* We are listening on one port now, no connections because the
     [Rpc_parallel.Heartbeater] only connects if [spawn] was called. *)
  let%bind () = assert_fds [%here] ~listen:1 ~established:0 in
  let%bind connection = Worker.Connection.client_exn worker () in
  (* Just the single connection we just made, but double counted because both ends are in
     this process *)
  let%bind () = assert_fds [%here] ~listen:1 ~established:2 in
  let%bind () =
    Worker.Connection.run_exn connection ~f:Rpc_parallel.Function.close_server ~arg:()
  in
  (* Existing connections to the in-process worker server remain open *)
  let%bind () = assert_fds [%here] ~listen:0 ~established:2 in
  let%bind () = Worker.Connection.close connection in
  let%bind () = Worker.Connection.close_finished connection in
  (* This closed both counts of that connection *)
  assert_fds [%here] ~listen:0 ~established:0
;;

let test_managed () =
  let%bind worker =
    Worker_managed.spawn_exn
      ~redirect_stderr:`Dev_null
      ~redirect_stdout:`Dev_null
      ~on_failure:Error.raise
      ~on_connection_to_worker_closed:Error.raise
      ()
      ()
  in
  (* We have the [Rpc_parallel.Heartbeater] connection along with the initial connection
     (that we keep because it is a managed [Worker.t]) *)
  let%bind () = assert_fds [%here] ~listen:1 ~established:2 in
  let%bind () = Worker_managed.run_exn worker ~f:Worker_managed.functions.ping ~arg:() in
  (* We should be reusing the same connection *)
  let%bind () = assert_fds [%here] ~listen:1 ~established:2 in
  let%bind () = Worker_managed.kill_exn worker in
  (* Now we should have no connections again *)
  assert_fds [%here] ~listen:1 ~established:0
;;

let command =
  Command.async_spec
    ~summary:"Fd testing Rpc parallel"
    Command.Spec.empty
    (fun () ->
      (* make sure we are not starting servers until we need them *)
      let%bind () = assert_fds [%here] ~listen:0 ~established:0 in
      let%bind () = test_serve () in
      let%bind () = test_unmanaged () in
      let%map () = test_managed () in
      printf "Ok\n")
    ~behave_nicely_in_pipeline:false
;;

let () = Rpc_parallel_krb_public.start_app ~krb_mode:For_unit_test command
