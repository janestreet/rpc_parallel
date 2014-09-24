open Core.Std
open Async.Std
open Rpc_parallel.Std

module Pre_worker = struct
  type 'w functions = {
    getenv : ('w, string, string option) Parallel.Function.t
  }

  type init_arg = unit with bin_io

  let init = return

  module Functions(C : Parallel.Creator) = struct
    let getenv =
      C.create_rpc ~bin_input:String.bin_t ~bin_output:(Option.bin_t String.bin_t)
        ~f:(fun key -> return (Unix.getenv key)) ()

    let functions = { getenv }
  end
end

module Worker = Parallel.Make_worker(Pre_worker)

let command =
  Command.async ~summary:"Using environment variables for great good"
    Command.Spec.(empty
                  +> flag "host" (optional string) ~doc:"HOST run worker on HOST")
    (fun host () ->
       let key = "TEST_ENV_KEY" in
       let data = "potentially \"problematic\" \\\"test\\\" string ()!" in
       begin
         match host with
         | None -> return `Local
         | Some host ->
           Parallel.Remote_executable.copy_to_host ~executable_dir:"~" host
           >>| fun remote_executable ->
           `Remote (Or_error.ok_exn remote_executable)
       end
       >>= fun where ->
       Worker.spawn_exn ~env:[key, data] ~where () ~on_failure:Error.raise
       >>= fun worker ->
       Worker.run_exn worker ~f:Worker.functions.getenv ~arg:key
       >>= fun result ->
       assert (result = Some data);
       Worker.run_exn worker ~f:Worker.functions.getenv ~arg:"SHOULD_NOT_EXIST"
       >>| fun result ->
       assert (result = None);
       printf "success!\n")

let () = Parallel.start_app command
