open Core
open Async

module Worker_id = Uuid
module Worker_type_id = Unique_id.Int ()

module Internal_connection_state = struct
  type ('worker_state, 'conn_state) t1 =
    { worker_state : 'worker_state
    ; conn_state   : 'conn_state
    ; worker_id    : Worker_id.t }

  type ('worker_state, 'conn_state) t =
    Rpc.Connection.t * ('worker_state, 'conn_state) t1 Set_once.t
end

let try_within ~monitor f =
  let ivar = Ivar.create () in
  Scheduler.within ~monitor (fun () ->
    Monitor.try_with ~run:`Now ~rest:`Raise f
    >>> fun r ->
    Ivar.fill ivar (Result.map_error r ~f:Error.of_exn));
  Ivar.read ivar

let try_within_exn ~monitor f =
  try_within ~monitor f
  >>| function
  | Ok x -> x
  | Error e -> Error.raise e

(* Use /proc/PID/exe to get the currently running executable.
   - argv[0] might have been deleted (this is quite common with jenga)
   - `cp /proc/PID/exe dst` works as expected while `cp /proc/self/exe dst` does not *)
let our_binary =
  let our_binary_lazy =
    lazy (Unix.getpid () |> Pid.to_int |> sprintf "/proc/%d/exe")
  in
  fun () -> Lazy.force our_binary_lazy

let our_md5 =
  let our_md5_lazy =
    lazy begin
      Process.run ~prog:"md5sum" ~args:[our_binary ()] ()
      >>|? fun our_md5 ->
      let our_md5, _ = String.lsplit2_exn ~on:' ' our_md5 in
      our_md5
    end
  in fun () -> Lazy.force our_md5_lazy

let is_child_env_var = "ASYNC_PARALLEL_IS_CHILD_MACHINE"

let whoami () =
  match Sys.getenv is_child_env_var with
  | Some _ -> `Worker
  | None -> `Master

let clear_env () = Unix.unsetenv is_child_env_var

let validate_env env =
  match List.find env ~f:(fun (key, _) -> key = is_child_env_var) with
  | Some e ->
    Or_error.error
      "Environment variable conflicts with Rpc_parallel machinery"
      e [%sexp_of: string*string]
  | None -> Ok ()

let create_worker_env ~extra =
  let open Or_error.Monad_infix in
  validate_env extra
  >>| fun () ->
  extra @ [is_child_env_var, ""]

let to_daemon_fd_redirection = function
  | `Dev_null -> `Dev_null
  | `File_append s -> `File_append s
  | `File_truncate s -> `File_truncate s
