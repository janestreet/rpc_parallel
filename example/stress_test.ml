open Core.Std
open Async.Std
open Rpc_parallel.Std

module Worker = struct
  module T = struct
    type 'worker functions = {ping:('worker, unit, unit) Parallel.Function.t}

    type init_arg = unit [@@deriving bin_io]
    type state = unit
    let init  = return

    module Functions(C:Parallel.Creator with type state := state) = struct
      let ping =
        C.create_rpc ~f:(fun () -> return) ~bin_input:Unit.bin_t ~bin_output:Unit.bin_t ()

      let functions = {ping}
    end
  end
  include Parallel.Make_worker(T)
end

let command =
  Command.async ~summary:"Simple use of Async Parallel V2"
    Command.Spec.(
      empty
      +> flag "-worker" (optional string) ~doc:" worker to run test on"
      +> flag "-dir" (optional_with_default "~" string)
           ~doc:" directory to copy executable to"
      +> flag "-num-workers" (optional_with_default 20 int)
           ~doc:" number of workers to spawn"
      +> flag "-num-loops" (optional_with_default 30 int)
           ~doc:" number of loops to test"
    )
    (fun worker dir num_workers num_loops () ->
       let setup =
         match worker with
         | None -> return (`Local, fun () -> return ())
         | Some w ->
           Parallel.Remote_executable.copy_to_host ~executable_dir:dir w
           >>| function
           | Error e -> Error.raise e
           | Ok exec -> `Remote exec, fun () -> Parallel.Remote_executable.delete exec
                          >>| Or_error.ok_exn
       in
       setup >>= fun (executable, cleanup) ->
       let rec loop remaining =
         if remaining = 0 then return ()
         else
           begin
             let start = Time.to_float (Time.now ()) in
             Deferred.all_unit (List.map (List.range 0 num_workers) ~f:(fun _i ->
               Worker.spawn_exn ~where:executable ~redirect_stdout:`Dev_null
                 ~redirect_stderr:`Dev_null () ~on_failure:Error.raise
               >>= fun worker ->
               Worker.run_exn worker ~f:Worker.functions.ping ~arg:() >>= fun () ->
               Worker.kill_exn worker))
             >>= fun () ->
             let end_ = Time.to_float (Time.now ()) in
             Core.Std.Printf.printf "%f\n%!" (end_ -. start); loop (remaining - 1)
           end
       in
       loop num_loops
       >>= fun () ->
       cleanup ()
    )

let () = Parallel.start_app command
