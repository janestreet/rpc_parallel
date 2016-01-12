open Core.Std
open Async.Std
open Rpc_parallel.Std
open Qtest_lib.Std

module Add_map_function = Map_reduce.Make_map_function_with_init(struct
  module Param = Int
  module Input = Int
  module Output = Int
  type state_type = int
  let init = return
  let map state x = return (x + state)
end)

module Count_map_reduce_function =
  Map_reduce.Make_map_reduce_function_with_init(struct
    module Param = Int
    module Accum = Int
    module Input = struct
      type t = int list [@@deriving bin_io]
    end
    type state_type = int
    let init = return
    let map state l = return (state * List.fold l ~init:0 ~f:(+))
    let combine _state x y = return (x + y)
  end)

module Concat_map_reduce_function = Map_reduce.Make_map_reduce_function(struct
  module Accum = struct
    type t = int list [@@deriving bin_io]
  end
  module Input = Int
  let map x = return [x]
  let combine l1 l2 = return (l1 @ l2)
end)

let test_map_unordered () =
  let n = 1000 in
  let input = List.init n ~f:Fn.id in
  Map_reduce.map_unordered (Map_reduce.Config.create ~local:5 ())
    (Pipe.of_list input) ~m:(module Add_map_function) ~param:n
  >>= Pipe.to_list
  >>= fun output ->
  let numbers = List.sort (List.map output ~f:fst) ~cmp:Int.compare in
  let expected_numbers = List.init n ~f:((+) n) in
  let indices = List.sort (List.map output ~f:snd) ~cmp:Int.compare in
  let expected_indices = input in
  assert (List.equal indices expected_indices ~equal:(=));
  assert (List.equal numbers expected_numbers ~equal:(=));
  Deferred.unit

let test_map () =
  let n = 1000 in
  let input = List.init n ~f:Fn.id in
  Map_reduce.map (Map_reduce.Config.create ~local:5 ())
    (Pipe.of_list input) ~m:(module Add_map_function) ~param:n
  >>= Pipe.to_list
  >>= fun output ->
  let expected_output = List.init n ~f:((+) n) in
  assert (List.equal output expected_output ~equal:(=));
  Deferred.unit

let test_map_reduce_commutative () =
  let n = 1000 in
  let multiplier = 2 in
  let input = List.init n ~f:(fun m -> List.init m ~f:Fn.id) in
  Map_reduce.map_reduce_commutative (Map_reduce.Config.create ~local:5 ())
    (Pipe.of_list input) ~m:(module Count_map_reduce_function) ~param:multiplier
  >>= fun sum ->
  assert (Option.value_exn sum =
          multiplier * (List.fold ~init:0 ~f:(+)
                          (List.map input ~f:(List.fold ~init:0 ~f:(+)))));
  Deferred.unit

let test_map_reduce () =
  let n = 1000 in
  let input = List.init n ~f:Fn.id in
  Map_reduce.map_reduce (Map_reduce.Config.create ~local:5 ())
    (Pipe.of_list input) ~m:(module Concat_map_reduce_function) ~param:()
  >>= fun l ->
  assert (Option.value_exn l = input);
  Deferred.unit

let tests =
  [ ("map_unordered", test_map_unordered)
  ; ("map", test_map)
  ; ("map_reduce_commutative", test_map_reduce_commutative)
  ; ("map_reduce", test_map_reduce)
  ]

let () = Parallel.start_app (Command.basic Command.Spec.empty ~summary:"Run tests"
                               (fun () -> Runner.main ~check_fds:false tests))
