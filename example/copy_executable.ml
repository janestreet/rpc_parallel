open Core.Std
open Async.Std
open Rpc_parallel.Std

(* Tests for the [Remote_executable] module *)

module Worker = struct
  module T = struct
    type 'worker functions = {ping:('worker, unit, unit) Parallel.Function.t}

    type init_arg = unit [@@deriving bin_io]
    type state = unit

    let init = return

    module Functions(C:Parallel.Creator with type state := state) = struct

      let ping = C.create_rpc ~f:(fun () -> return)
                   ~bin_input:Unit.bin_t ~bin_output:Unit.bin_t ()

      let functions = {ping}
    end
  end
  include Parallel.Make_worker(T)
end

(* Copy an executable to a remote host *)
let copy_to_host_test worker dir =
  Parallel.Remote_executable.copy_to_host ~executable_dir:dir worker
  >>= function
  | Error e -> Error.raise e
  | Ok executable ->
    Worker.spawn_exn ~where:(`Remote executable) ~redirect_stdout:`Dev_null
      ~redirect_stderr:`Dev_null () ~on_failure:Error.raise >>= fun w ->
    Worker.run_exn w ~f:Worker.functions.ping ~arg:() >>| fun () ->
    executable

(* Spawn a worker using an existing remote executable *)
let existing_on_host_test worker path =
  let existing_executable =
    Parallel.Remote_executable.existing_on_host ~executable_path:path worker
  in
  Worker.spawn_exn ~where:(`Remote existing_executable) ~redirect_stdout:`Dev_null
    ~redirect_stderr:`Dev_null () ~on_failure:Error.raise
  >>= fun w2 ->
  Worker.run_exn w2 ~f:Worker.functions.ping ~arg:()

(* Make sure the library appropriately fails when trying to spawn a worker from a
   mismatching executable *)
let mismatching_executable_test worker dir =
  Parallel.Remote_executable.copy_to_host ~executable_dir:dir worker
  >>= function
  | Error e -> Error.raise e
  | Ok executable ->
    let path = Parallel.Remote_executable.path executable in
    Process.run ~prog:"ssh"
      ~args:["-o"; "StrictHostKeyChecking=no"; worker; sprintf "echo 0 >> %s" path] ()
    >>= function
    | Error e -> Error.raise e
    | Ok (_:string) ->
      Worker.spawn ~where:(`Remote executable) ~redirect_stdout:`Dev_null
        ~redirect_stderr:`Dev_null () ~on_failure:Error.raise
      >>= function
      | Error e ->
        let error = Error.to_string_hum e in
        let expected =
          sprintf "The remote executable %s does not match the local executable"
            path
        in
        assert (error = expected);
        Parallel.Remote_executable.delete executable
        >>| Or_error.ok_exn
      | Ok (_:Worker.t) -> assert false

let delete_test executable =
  Parallel.Remote_executable.delete executable >>| Or_error.ok_exn

let command =
  Command.async ~summary:"Simple use of Async Parallel V2"
    Command.Spec.(
      empty
      +> flag "-worker" (required string) ~doc:"worker to run copy test on"
      +> flag "-dir" (required string) ~doc:"directory to copy executable to"
    )
    (fun worker dir () ->
       let get_count path =
         Process.run ~prog:"ssh" ~args:["-o"; "StrictHostKeyChecking=no";worker;
                                        sprintf "find %s* | wc -l" path] ()
         >>| fun res_or_err ->
         Or_error.ok_exn res_or_err
         |! String.strip
         |! Int.of_string
       in
       Unix.readlink (sprintf "/proc/%d/exe" (Pid.to_int (Unix.getpid ())))
       >>= fun our_binary ->
       let filename = Filename.basename our_binary in
       get_count (dir ^/ filename) >>= fun old_count ->
       copy_to_host_test worker dir
       >>= fun executable ->
       existing_on_host_test worker (Parallel.Remote_executable.path executable)
       >>= fun () ->
       mismatching_executable_test worker dir
       >>= fun () ->
       delete_test executable
       >>= fun () ->
       get_count (dir ^/ filename) >>| fun new_count ->
       assert (old_count = new_count)
    )

let () = Parallel.start_app command
