open Core
open Async

module Worker = struct
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

    module Functions (_ : Rpc_parallel.Creator) = struct
      let functions = ()
      let init_worker_state () = Deferred.unit

      let init_connection_state ~connection:_ ~worker_state:_ =
        raise_s [%message "[init_connection_state] failure"]
      ;;
    end
  end

  include Rpc_parallel.Make (T)
end

let spawn_in_foreground ?connection_timeout () =
  match%bind
    Worker.For_internal_testing.spawn_in_foreground
      ?connection_timeout
      ~connection_state_init_arg:()
      ~on_failure:Error.raise
      ~shutdown_on:Connection_closed
      ()
  with
  | Ok (_, _) -> failwith "Unexpected success while spawning worker"
  | Error (_, `Worker_process worker_process) ->
    (match worker_process with
     | None -> Deferred.unit
     | Some exit_or_signal -> exit_or_signal >>| (ignore : Unix.Exit_or_signal.t -> unit))
;;

let pgrep_children ~pid (f : Procfs_async.Process.t -> bool) =
  let open Procfs_async in
  pgrep (fun process ->
    return ([%compare.equal: Pid.t option] process.stat.ppid (Some pid) && f process))
;;

let is_zombie process =
  process
  |> Procfs_async.Process.stat
  |> Procfs_async.Process.Stat.state
  |> Procfs_async.Process.Stat.State.equal Zombie
;;

let wait_until_no_running_children ~pid =
  Deferred.repeat_until_finished () (fun () ->
    let%bind non_zombie_processes =
      pgrep_children ~pid (fun process -> not (is_zombie process))
    in
    if List.is_empty non_zombie_processes
    then return (`Finished ())
    else return (`Repeat ()))
;;

let print_zombies ~pid =
  match%map pgrep_children ~pid is_zombie with
  | [] -> print_s [%message "No zombies"]
  | zombies -> print_s [%message "Zombies" (zombies : Procfs_async.Process.t list)]
;;

let test ?connection_timeout () =
  let pid = Unix.getpid () in
  let%bind () = spawn_in_foreground ?connection_timeout () in
  let%bind () = wait_until_no_running_children ~pid in
  let%bind () = print_zombies ~pid in
  Deferred.unit
;;

let main () =
  (* By setting [connection_timeout] to [Time.Span.zero], we force a failure in the part
     of the worker initialization where the master waits for the recently spawned worker
     to connect back. *)
  let%bind () = test ~connection_timeout:Time_float.Span.zero () in
  let%bind () = test () in
  Deferred.unit
;;

let command =
  Command.async
    ~summary:"[spawn_in_foreground]: ensure no zombies"
    (Command.Param.return main)
    ~behave_nicely_in_pipeline:false
;;

let () = Rpc_parallel_krb_public.start_app ~krb_mode:For_unit_test command
