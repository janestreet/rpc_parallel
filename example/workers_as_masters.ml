open Core.Std
open Async.Std
open Rpc_parallel.Std

(* An example demonstrating how workers can themselves act as masters and spawn more
   workers. We have to layers of workers, where the first layer spawns the workers of the
   second layer. *)

module Secondary_worker = struct
  module T = struct
    type 'worker functions =
      { ping:('worker, unit, string) Parallel.Function.t
      }

    type init_arg = unit with bin_io
    type state = unit

    let init = return

    module Functions(C:Parallel.Creator with type state := state) = struct
      let ping_impl () () = return "pong"

      let ping =
        C.create_rpc ~f:ping_impl ~bin_input:Unit.bin_t ~bin_output:String.bin_t ()

      let functions = {ping}
    end
  end
  include Parallel.Make_worker(T)
end

module Primary_worker = struct
  module T = struct
    type ping_result = string list with bin_io
    type 'worker functions =
      { run:('worker, int, unit) Parallel.Function.t
      ; ping:('worker, unit, ping_result) Parallel.Function.t
      }

    let workers = Bag.create ()
    let next_worker_name () = sprintf "Secondary worker #%i" (Bag.length workers)

    type init_arg = unit with bin_io
    type state = unit

    let init () =
      Bag.clear workers;
      return ()

    module Functions(C:Parallel.Creator with type state := state) = struct
      let run_impl () num_workers =
        Deferred.List.init ~how:`Parallel num_workers ~f:(fun _i ->
          Secondary_worker.spawn_exn () ~on_failure:Error.raise
          >>| fun secondary_worker ->
          ignore(Bag.add workers (next_worker_name (), secondary_worker));
        )
        >>| ignore

      let run = C.create_rpc ~f:run_impl ~bin_input:Int.bin_t ~bin_output:Unit.bin_t ()

      let ping_impl () () =
        Deferred.List.map ~how:`Parallel (Bag.to_list workers) ~f:(fun (name, worker) ->
          Secondary_worker.run worker ~arg:() ~f:Secondary_worker.functions.ping
          >>| function
          | Error e -> sprintf "%s: failed (%s)" name (Error.to_string_hum e)
          | Ok s -> sprintf "%s: %s" name s
        )

      let ping =
        C.create_rpc ~f:ping_impl ~bin_input:Unit.bin_t ~bin_output:bin_ping_result ()

      let functions = {run; ping}
    end
  end
  include Parallel.Make_worker(T)
end

let command =
  (* Make sure to always use [Command.async] *)
  Command.async ~summary:"Simple use of Async Parallel V2"
    Command.Spec.(
      empty
      +> flag "primary" (required int)
           ~doc:" Number of primary workers to spawn"
      +> flag "secondary" (required int)
           ~doc:" Number of secondary workers each primary worker should spawn"
    )
    (fun primary secondary () ->
       Deferred.List.init ~how:`Parallel primary ~f:(fun worker_id ->
         Primary_worker.spawn_exn () ~on_failure:Error.raise
         >>= fun primary_worker ->
         Primary_worker.run_exn primary_worker
           ~f:Primary_worker.functions.run ~arg:secondary
         >>= fun () ->
         Primary_worker.run_exn primary_worker
           ~f:Primary_worker.functions.ping ~arg:()
         >>| fun ping_results ->
         List.map ping_results ~f:(fun s -> sprintf "Primary worker #%i: %s" worker_id s)
       )
       >>| List.join
       >>| fun ping_results ->
       List.iter ping_results ~f:(printf "%s\n%!")
    )

(* This call to [Parallel.start_app] must be top level *)
let () = Parallel.start_app command
