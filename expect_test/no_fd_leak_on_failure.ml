open Core
open Async

module T = struct
  type 'worker functions = unit

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
    let functions = ()
    let init_worker_state () = failwith "fail"
    let init_connection_state ~connection:_ ~worker_state:_ = return
  end
end

include Rpc_parallel.Make (T)

let fds () =
  let pid = Unix.getpid () |> Pid.to_string in
  Process.run_lines_exn () ~prog:"/usr/sbin/lsof" ~args:[ "-p"; pid ]
;;

let _check_fds () =
  let run_and_get_fds () =
    match%bind
      For_internal_testing.spawn_in_foreground
        ~on_failure:Error.raise
        ~shutdown_on:Connection_closed
        ~connection_state_init_arg:()
        ()
    with
    | Ok _ -> failwith "expected failure"
    | Error (_, `Worker_process worker_process) ->
      let%bind () =
        match worker_process with
        | None -> return ()
        | Some exit_or_signal ->
          exit_or_signal >>| (ignore : Unix.Exit_or_signal.t -> unit)
      in
      fds ()
  in
  let%bind baseline = run_and_get_fds () in
  let%bind fds_after_each_run = Deferred.List.init 5 ~f:(fun _ -> run_and_get_fds ()) in
  List.map fds_after_each_run ~f:(fun fds_after_run ->
    if List.length fds_after_run = List.length baseline
    then Ok ()
    else
      Or_error.error_s
        [%message "Fd leak?" (baseline : string list) (fds_after_run : string list)])
  |> Or_error.all_unit
  |> [%sexp_of: unit Or_error.t]
  |> print_s;
  return ()
;;

let () = Rpc_parallel.For_testing.initialize [%here]

(* let%expect_test "" =
 *   let%bind () = check_fds () in
 *   (* The stdout/stdin/stderr of the child process are closed when the process fails. *)
 *   [%expect {| (Ok ()) |}];
 *   return ()
 * ;; *)
