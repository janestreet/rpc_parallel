open Core
open Async

module Add_numbers_map_function = Rpc_parallel.Map_reduce.Make_map_function (struct
    module Input = struct
      type t = int * int [@@deriving bin_io]
    end

    module Output = struct
      type t = int * int [@@deriving bin_io]
    end

    let rec spin ntimes =
      match ntimes with
      | 0 -> ()
      | _ -> spin (ntimes - 1)
    ;;

    let map (index, max) =
      spin 100000000;
      (* Waste some CPU time *)
      return (index, List.fold ~init:0 ~f:( + ) (List.init max ~f:Fn.id))
    ;;
  end)

let command =
  Command.async_spec
    ~summary:"Add numbers in parallel"
    Command.Spec.(
      empty
      +> flag "max" (required int) ~doc:" Number to add up to"
      +> flag
           "ntimes"
           (optional_with_default 1000 int)
           ~doc:" Number of times to repeat the operation"
      +> flag "nworkers" (optional_with_default 4 int) ~doc:" Number of workers"
      +> flag "ordered" (optional_with_default true bool) ~doc:" Ordered or unordered")
    (fun max ntimes nworkers ordered () ->
      let list = Pipe.of_list (List.init ntimes ~f:(fun i -> i, max)) in
      let config =
        Rpc_parallel.Map_reduce.Config.create
          ~local:nworkers
          ~redirect_stderr:`Dev_null
          ~redirect_stdout:`Dev_null
          ()
      in
      if ordered
      then (
        let%bind output_reader =
          Rpc_parallel.Map_reduce.map
            config
            list
            ~m:(module Add_numbers_map_function)
            ~param:()
        in
        Pipe.iter output_reader ~f:(fun (index, sum) ->
          printf "%i: %i\n" index sum;
          Deferred.unit))
      else (
        let%bind output_reader =
          Rpc_parallel.Map_reduce.map_unordered
            config
            list
            ~m:(module Add_numbers_map_function)
            ~param:()
        in
        Pipe.iter output_reader ~f:(fun ((index, sum), mf_index) ->
          assert (index = mf_index);
          printf "%i:%i: %i\n" mf_index index sum;
          Deferred.unit)))
    ~behave_nicely_in_pipeline:false
;;

let () = Rpc_parallel_krb_public.start_app ~krb_mode:For_unit_test command
