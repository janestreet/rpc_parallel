open Core.Std
open Async.Std
open Rpc_parallel.Std

module Worker = struct
  module T = struct
    type 'worker functions = {ret:('worker, unit, unit) Parallel.Function.t}

    type init_arg = unit [@@deriving bin_io]
    type state = unit

    let init = return

    module Functions(C:Parallel.Creator with type state := state) = struct
      let ret = C.create_rpc ~f:(fun () -> return)
                  ~bin_input:Unit.bin_t ~bin_output:Unit.bin_t ()

      let functions = {ret}
    end
  end
  include Parallel.Make_worker(T)
end

let command =
  Command.async ~summary:"Testing of error reporting for bad cd and \
                          redirect_std(out|err) arguments for spawn"
    Command.Spec.(
      empty
      +> flag "cd" (required string)
           ~doc:"DIR working directory of spawned worker"
      +> flag "redirect-stdout" (required string) ~doc:"FILE stdout redirection"
      +> flag "redirect-stderr" (required string) ~doc:"FILE stderr redirection"
    )
    (fun cd redirect_stdout redirect_stderr () ->
       let redirect_stdout = `File_truncate redirect_stdout in
       let redirect_stderr = `File_truncate redirect_stderr in
       Worker.spawn_exn
         ~where:`Local
         ~cd
         ~redirect_stdout
         ~redirect_stderr
         ()
         ~on_failure:Error.raise
       >>= fun worker ->
       Worker.run_exn worker ~f:Worker.functions.ret ~arg:()
       >>| fun () ->
       printf "Success.\n")

let () = Parallel.start_app command
