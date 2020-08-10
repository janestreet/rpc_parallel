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

let count_fds () =
  let pid = Unix.getpid () |> Pid.to_string in
  Process.run_lines_exn () ~prog:"/usr/sbin/lsof" ~args:[ "-p"; pid ] >>| List.length
;;

let print_fd_counts () =
  let run_and_count_fds () =
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
      count_fds ()
  in
  let%bind baseline = run_and_count_fds () in
  let%bind fd_counts = Deferred.List.init 5 ~f:(fun _ -> run_and_count_fds ()) in
  let fd_counts = List.map fd_counts ~f:(fun fd_count -> fd_count - baseline) in
  print_s [%sexp (fd_counts : int list)];
  return ()
;;

let () = Rpc_parallel.For_testing.initialize [%here]

let%expect_test "" =
  let%bind () = print_fd_counts () in
  (* The stdout/stdin/stderr of the child process are closed when the process fails. *)
  [%expect {| (0 0 0 0 0) |}]
;;
