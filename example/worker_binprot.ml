open Core.Std
open Async.Std
open Rpc_parallel.Std

(* This demonstrates how to take advantage of the feature in the [Rpc_parallel]
   library that a [Worker.t] type is defined [with bin_io]. It is easy to define a
   [Worker1.t] type and then a [Worker2.t] type that implements a function
   [Worker1.t -> 'a]. However, it is also easy for some worker type to implement a function
   that takes its own type as an argument (as demonstrated here). *)

module Worker = struct
  module T = struct
    (* A [Worker.t] implements two functions: [ping : unit -> int] and
       [dispatch : Worker.t -> int] *)
    type 'worker functions =
      {ping:('worker, unit, int) Parallel.Function.t;
       dispatch: ('worker, 'worker, int) Parallel.Function.t}

    (* Don't do any initialization *)
    type init_arg = unit with bin_io
    type state = unit
    let init = return

    (* Internal state for each [Worker.t]. Every [Worker.t] has a counter that gets
       incremented anytime it gets pinged *)
    let counter = ref 0

    module Functions(C:Parallel.Creator with type state := state) = struct
      (* When a worker gets a [ping ()] call, increment its counter and return the current
         value *)
      let ping_impl () () = incr counter; return (!counter)
      let ping = C.create_rpc ~f:ping_impl ~bin_input:Unit.bin_t ~bin_output:Int.bin_t ()

      (* When a worker gets a [dispatch worker] call, call [ping] on the supplied worker
         and return the same result. *)
      let dispatch_impl () worker = C.run_exn worker ~f:ping ~arg:()
      let dispatch =
        C.create_rpc ~f:dispatch_impl ~bin_input:C.bin_worker ~bin_output:Int.bin_t ()

      let functions = {ping; dispatch}
    end
  end
  include Parallel.Make_worker(T)
end

let command =
  Command.async ~summary:
    "Example of a worker taking in another worker as an argument to one of its functions"
    Command.Spec.(
      empty
    )
    (fun () ->
       Worker.spawn_exn () ~on_failure:Error.raise >>= fun worker1 ->
       Worker.spawn_exn () ~on_failure:Error.raise >>= fun worker2 ->
       let repeat job n =
         Deferred.List.iter (List.range 0 n) ~f:(fun _i -> job ())
       in
       Deferred.all_unit [
         repeat (fun () ->
           Worker.run_exn worker1 ~f:Worker.functions.ping ~arg:()
           >>= fun count ->
           Core.Std.Printf.printf "worker1 pinged from master: %d\n%!" count;
           return ()) 10;
         repeat (fun () ->
           Worker.run_exn worker1 ~f:Worker.functions.dispatch
             ~arg:worker2
           >>= fun count ->
           Core.Std.Printf.printf "worker2 pinged from worker1: %d\n%!" count;
           return ()) 10
       ])

let () = Parallel.start_app command
