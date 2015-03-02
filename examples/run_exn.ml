open Core.Std
open Async.Std
open Rpc_parallel.Std

module Failer_impl = struct
  type 'a functions = ('a, unit, unit) Parallel.Function.t

  type init_arg = unit with bin_io
  type state = unit

  let init = return

  module Functions (C : Parallel.Creator with type state := state) = struct
    let functions =
      C.create_rpc
        ~bin_input:Unit.bin_t
        ~bin_output:(Unit.bin_t)
        ~f:(fun () () -> failwith "text of expected failure")
        ()
  end
end

module Failer = Parallel.Make_worker (Failer_impl)

let command =
  Command.async
    ~summary:"ensure that raising in a worker function passes the exception to the master"
    Command.Spec.empty
    (fun () ->
       Failer.spawn_exn ~on_failure:Error.raise ()
       >>= fun failer ->
       Failer.run failer ~f:Failer.functions ~arg:()
       >>| function
       | Ok () -> failwith "expected to fail but did not"
       | Error error ->
         printf "expected failure: %s\n" (Error.to_string_hum error);)
;;

let () = Parallel.start_app command
