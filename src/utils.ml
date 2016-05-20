open Core.Std
open Async.Std

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
    Monitor.try_with ~extract_exn:true ~run:`Now ~rest:`Raise f
    >>> fun r ->
    Ivar.fill ivar (Result.map_error r ~f:Error.of_exn));
  Ivar.read ivar

let try_within_exn ~monitor f =
  try_within ~monitor f
  >>| function
  | Ok x -> x
  | Error e -> Error.raise e

let our_binary =
  let our_binary_lazy =
    lazy (Unix.getpid () |> Pid.to_int |> sprintf "/proc/%d/exe" |> Unix.readlink)
  in
  fun () -> Lazy.force our_binary_lazy

let our_md5 =
  let our_md5_lazy =
    lazy begin
      our_binary ()
      >>= fun binary ->
      Process.run ~prog:"md5sum" ~args:[binary] ()
      >>|? fun our_md5 ->
      let our_md5, _ = String.lsplit2_exn ~on:' ' our_md5 in
      our_md5
    end
  in fun () -> Lazy.force our_md5_lazy

let is_child_env_var = "ASYNC_PARALLEL_IS_CHILD_MACHINE"

let whoami () =
  match Sys.getenv is_child_env_var with
  | Some id_str -> `Worker id_str
  | None -> `Master

let clear_env () = Unix.unsetenv is_child_env_var

let validate_env env =
  match List.find env ~f:(fun (key, _) -> key = is_child_env_var) with
  | Some e ->
    Or_error.error
      "Environment variable conflicts with Rpc_parallel machinery"
      e [%sexp_of: string*string]
  | None -> Ok ()

let create_worker_env ~extra ~id =
  let open Or_error.Monad_infix in
  validate_env extra
  >>| fun () ->
  extra @ [is_child_env_var, id]

let to_daemon_fd_redirection = function
  | `Dev_null -> `Dev_null
  | `File_append s -> `File_append s
  | `File_truncate s -> `File_truncate s
