open Core.Std
open Async.Std
open Rpc_parallel.Std

module Side_arg_map_function = Map_reduce.Make_map_function_with_init(struct
  type state_type = string
  module Param = struct
    type t = string [@@deriving bin_io]
  end
  module Input = struct
    type t = unit [@@deriving bin_io]
  end
  module Output = struct
    type t = string [@@deriving bin_io]
  end

  let init param =
    Random.self_init ();
    return (sprintf "[%i] %s" (Random.bits ()) param)
  let map state () =
    return state
end)

let command =
  Command.async ~summary:"Pass a side arg"
    Command.Spec.(
      empty
      +> flag "ntimes" (optional_with_default 100 int) ~doc:" Number of things to map"
      +> flag "nworkers" (optional_with_default 4 int) ~doc:" Number of workers"
    )
    (fun ntimes nworkers () ->
       let list = (Pipe.of_list (List.init ntimes ~f:(fun _i -> ()))) in
       Map_reduce.map_unordered
         (Map_reduce.Config.create ~local:nworkers ())
         list
         ~m:(module Side_arg_map_function)
         ~param:"Message from the master"
       >>= fun output_reader ->
       Pipe.iter output_reader ~f:(fun (message, index) ->
         printf "%i: %s\n" index message;
         Deferred.unit
       )
    )

let () = Parallel.start_app command
