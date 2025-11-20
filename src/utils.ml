open Core
open Poly
open Async

module Worker_id = struct
  let create = Uuid_unix.create

  (* If we do not use the stable sexp serialization, when running inline tests, we will
     create UUIDs that fail tests *)
  module T = Uuid.Stable.V1

  type t = T.t [@@deriving sexp, bin_io]

  include Comparable.Make_binable (T)
  include Hashable.Make_binable (T)
  include Sexpable.To_stringable (T)

  let pp fmt t = String.pp fmt (Sexp.to_string ([%sexp_of: t] t))
end

module Worker_type_id = Unique_id.Int ()

module Internal_connection_state = struct
  type ('worker_state, 'conn_state) t1 =
    { worker_state : 'worker_state
    ; conn_state : 'conn_state
    ; worker_id : Worker_id.t
    }

  type ('worker_state, 'conn_state) t =
    Rpc.Connection.t * ('worker_state, 'conn_state) t1 Set_once.t
end

let try_within ~monitor f =
  let ivar = Ivar.create () in
  Scheduler.within ~monitor (fun () ->
    Monitor.try_with ~run:`Now ~rest:`Raise f
    >>> fun r -> Ivar.fill_exn ivar (Result.map_error r ~f:Error.of_exn));
  Ivar.read ivar
;;

let try_within_exn ~monitor f =
  match%map try_within ~monitor f with
  | Ok x -> x
  | Error e -> Error.raise e
;;

let our_md5 =
  let our_md5_lazy =
    lazy
      (Process.run ~prog:"md5sum" ~args:[ Current_exe.get_path () ] ()
       >>|? fun our_md5 ->
       let our_md5, _ = String.lsplit2_exn ~on:' ' our_md5 in
       our_md5)
  in
  fun () -> Lazy.force our_md5_lazy
;;

let is_child_env_var = "ASYNC_PARALLEL_IS_CHILD_MACHINE"

let whoami () =
  match Sys.getenv is_child_env_var with
  | Some _ -> `Worker
  | None -> `Master
;;

let clear_env () = (Unix.unsetenv [@ocaml.alert "-unsafe_multidomain"]) is_child_env_var

let validate_env env =
  match List.find env ~f:(fun (key, _) -> key = is_child_env_var) with
  | Some e ->
    Or_error.error
      "Environment variable conflicts with Rpc_parallel machinery"
      e
      [%sexp_of: string * string]
  | None -> Ok ()
;;

(* Don't run tests in the worker if we are running an expect test. A call to
   [Rpc_parallel.For_testing.initialize] will initialize the worker and start the Async
   scheduler. *)
let force_drop_inline_test =
  if Core.am_running_test then [ "FORCE_DROP_INLINE_TEST", "" ] else []
;;

let create_worker_env ~extra =
  let open Or_error.Let_syntax in
  let%map () = validate_env extra in
  extra
  @ force_drop_inline_test
  @ For_testing_internal.worker_environment ()
  @ [ is_child_env_var, "" ]
;;

let to_daemon_fd_redirection = function
  | `Dev_null -> `Dev_null
  | `File_append s -> `File_append s
  | `File_truncate s -> `File_truncate s
;;
