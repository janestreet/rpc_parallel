open Jane.Std
open Async.Std
open Rpc_parallel.Std

module Generate_random_map_function =
  Map_reduce.Make_map_function_with_init(struct
    type state_type = unit
    module Param = struct
      type t = unit [@@deriving bin_io]
    end
    module Input = struct
      type t = unit [@@deriving bin_io]
    end
    module Output = struct
      type t = float array [@@deriving bin_io]
    end

    let init () =
      Random.self_init ();
      Deferred.unit
    let map () () =
      return (Array.init 50000 ~f:(fun _ ->
        atan (atan (atan (atan (atan (atan (Random.float 100.))))))))
  end)

module Compute_stats_map_reduce_function =
  Map_reduce.Make_map_reduce_function(struct
    module Accum = struct
      type t = immutable Rstats.t [@@deriving bin_io]
    end
    module Input = struct
      type t = float array [@@deriving bin_io]
    end

    let map input =
      return (Array.fold input ~init:(Rstats.empty ())
                ~f:(fun acc x -> Rstats.update acc x))

    let combine acc1 acc2 =
      return (Rstats.merge acc1 acc2)
  end)

let command =
  Command.async ~summary:"Compute summary statistics in parallel"
    Command.Spec.(
      empty
      +> flag "nblocks" (optional_with_default 10000 int)
           ~doc:" Blocks to generate (total number of random numbers is 50000 * blocks)"
      +> flag "nworkers" (optional_with_default 4 int) ~doc:" Number of workers"
      +> flag "remote-host" (optional string) ~doc:" Remote host name"
      +> flag "remote-path" (optional string) ~doc:" Path to this exe on the remote host"
      +> flag "ordered" (optional_with_default true bool)
           ~doc:" Commutative or noncommutative fold (should not affect the result)"
    )
    (fun nblocks nworkers remote_host remote_path ordered () ->
       let config = match remote_host with
         | Some remote_host ->
           (match remote_path with
            | Some remote_path ->
              Map_reduce.Config.create
                ~remote:[ (Parallel.Remote_executable.existing_on_host
                             ~executable_path:remote_path remote_host, nworkers) ]
                ()
            | _ -> failwith "No remote path specified")
         | _ -> Map_reduce.Config.create ~local:nworkers ()
       in
       Map_reduce.map_unordered config
         (Pipe.of_list (List.init nblocks ~f:(Fn.const ())))
         ~m:(module Generate_random_map_function)
         ~param:()
       >>= fun blocks ->
       (if ordered then Map_reduce.map_reduce
        else Map_reduce.map_reduce_commutative)
         config
         (Pipe.map blocks ~f:(fun (block, _index) -> block))
         ~m:(module Compute_stats_map_reduce_function)
         ~param:()
       >>= function
       | Some stats ->
         printf "Samples: %i\n" (Rstats.samples stats);
         printf "Mean: %f\n" (Rstats.mean stats);
         printf "Variance: %f\n" (Rstats.var stats);
         Deferred.unit
       | None ->
         Deferred.unit
    )

let () = Parallel.start_app command
