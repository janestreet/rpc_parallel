open Core
open Poly
open Async

(* Tests for the [Remote_executable] module *)

module Worker = struct
  module T = struct
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

  include Rpc_parallel.Make (T)
end

(* Copy an executable to a remote host *)
let copy_to_host_test worker dir =
  Rpc_parallel.Remote_executable.copy_to_host ~executable_dir:dir worker
  >>=? fun executable ->
  Worker.spawn
    ~how:(Rpc_parallel.How_to_run.remote executable)
    ~shutdown_on:Connection_closed
    ~redirect_stdout:`Dev_null
    ~redirect_stderr:`Dev_null
    ~on_failure:Error.raise
    ~connection_state_init_arg:()
    ()
  >>=? fun conn ->
  Worker.Connection.run conn ~f:Worker.functions.ping ~arg:() >>|? fun () -> executable
;;

(* Spawn a worker using an existing remote executable *)
let existing_on_host_test worker path =
  let existing_executable =
    Rpc_parallel.Remote_executable.existing_on_host ~executable_path:path worker
  in
  Worker.spawn
    ~how:(Rpc_parallel.How_to_run.remote existing_executable)
    ~shutdown_on:Connection_closed
    ~redirect_stdout:`Dev_null
    ~redirect_stderr:`Dev_null
    ~on_failure:Error.raise
    ~connection_state_init_arg:()
    ()
  >>=? fun conn -> Worker.Connection.run conn ~f:Worker.functions.ping ~arg:()
;;

(* Make sure the library appropriately fails when trying to spawn a worker from a
   mismatching executable *)
let mismatching_executable_test worker dir =
  Rpc_parallel.Remote_executable.copy_to_host ~executable_dir:dir worker
  >>=? fun executable ->
  let path = Rpc_parallel.Remote_executable.path executable in
  (* When building with shared-cache, build artifacts in general and executables in
     particular are read-only. [cp] and [scp] preserve such permissions, which would cause
     the ">>" below to fail if we didn't chmod. *)
  Process.run
    ~prog:"ssh"
    ~args:
      [ "-o"
      ; "StrictHostKeyChecking=no"
      ; worker
      ; sprintf "chmod +w %s; echo 0 >> %s" path path
      ]
    ()
  >>=? fun _ ->
  match%bind
    Worker.spawn
      ~how:(Rpc_parallel.How_to_run.remote executable)
      ~shutdown_on:Heartbeater_connection_timeout
      ~redirect_stdout:`Dev_null
      ~redirect_stderr:`Dev_null
      ~on_failure:Error.raise
      ()
  with
  | Error e ->
    let error = Error.to_string_hum e in
    let expected =
      sprintf
        "The remote executable %s:%s does not match the local executable"
        worker
        path
    in
    assert (error = expected);
    Rpc_parallel.Remote_executable.delete executable
  | Ok (_ : Worker.worker) -> assert false
;;

let delete_test executable = Rpc_parallel.Remote_executable.delete executable

let command =
  Command.async_spec_or_error
    ~summary:"Simple use of Async Rpc_parallel V2"
    Command.Spec.(
      empty
      +> flag "-worker" (required string) ~doc:"worker to run copy test on"
      +> flag "-dir" (required string) ~doc:"directory to copy executable to")
    (fun worker dir () ->
      let get_count path =
        let%map res_or_err =
          Process.run
            ~prog:"ssh"
            ~args:
              [ "-o"
              ; "StrictHostKeyChecking=no"
              ; worker
              ; sprintf "find %s* | wc -l" path
              ]
            ()
        in
        Or_error.ok_exn res_or_err |> String.strip |> Int.of_string
      in
      let%bind our_binary =
        Unix.readlink (sprintf "/proc/%d/exe" (Pid.to_int (Unix.getpid ())))
      in
      let filename = Filename.basename our_binary in
      let%bind old_count = get_count (dir ^/ filename) in
      copy_to_host_test worker dir
      >>=? fun executable ->
      existing_on_host_test worker (Rpc_parallel.Remote_executable.path executable)
      >>=? fun () ->
      mismatching_executable_test worker dir
      >>=? fun () ->
      delete_test executable
      >>=? fun () ->
      let%map new_count = get_count (dir ^/ filename) in
      assert (old_count = new_count);
      Ok (printf "Ok\n"))
    ~behave_nicely_in_pipeline:false
;;

let () = Rpc_parallel_krb_public.start_app ~krb_mode:For_unit_test command
