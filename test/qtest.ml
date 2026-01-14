open Core
open Poly
open Async
open Qtest_deprecated.Std

module Add_map_function = Rpc_parallel.Map_reduce.Make_map_function_with_init (struct
    module Param = Int
    module Input = Int
    module Output = Int

    type state_type = int

    let init param ~worker_index:(_ : int) = return param
    let map state x = return (x + state)
  end)

module Count_map_reduce_function =
Rpc_parallel.Map_reduce.Make_map_reduce_function_with_init (struct
    module Param = Int
    module Accum = Int

    module Input = struct
      type t = int list [@@deriving bin_io]
    end

    type state_type = int

    let init param ~worker_index:(_ : int) = return param
    let map state l = return (state * List.fold l ~init:0 ~f:( + ))
    let combine _state x y = return (x + y)
  end)

module Concat_map_reduce_function =
Rpc_parallel.Map_reduce.Make_map_reduce_function (struct
    module Accum = struct
      type t = int list [@@deriving bin_io]
    end

    module Input = Int

    let map x = return [ x ]
    let combine l1 l2 = return (l1 @ l2)
  end)

let test_map_unordered () =
  let n = 1000 in
  let input = List.init n ~f:Fn.id in
  let config =
    Rpc_parallel.Map_reduce.Config.create
      ~local:5
      ~redirect_stderr:`Dev_null
      ~redirect_stdout:`Dev_null
      ()
  in
  let%bind output =
    Rpc_parallel.Map_reduce.map_unordered
      config
      (Pipe.of_list input)
      ~m:(module Add_map_function)
      ~param:n
    >>= Pipe.to_list
  in
  let numbers = List.sort (List.map output ~f:fst) ~compare:Int.compare in
  let expected_numbers = List.init n ~f:(( + ) n) in
  let indices = List.sort (List.map output ~f:snd) ~compare:Int.compare in
  let expected_indices = input in
  assert (List.equal ( = ) indices expected_indices);
  assert (List.equal ( = ) numbers expected_numbers);
  Deferred.unit
;;

let test_map () =
  let n = 1000 in
  let input = List.init n ~f:Fn.id in
  let config =
    Rpc_parallel.Map_reduce.Config.create
      ~local:5
      ~redirect_stderr:`Dev_null
      ~redirect_stdout:`Dev_null
      ()
  in
  let%bind output =
    Rpc_parallel.Map_reduce.map
      config
      (Pipe.of_list input)
      ~m:(module Add_map_function)
      ~param:n
    >>= Pipe.to_list
  in
  let expected_output = List.init n ~f:(( + ) n) in
  assert (List.equal ( = ) output expected_output);
  Deferred.unit
;;

let test_map_reduce_commutative () =
  let n = 1000 in
  let multiplier = 2 in
  let input = List.init n ~f:(fun m -> List.init m ~f:Fn.id) in
  let config =
    Rpc_parallel.Map_reduce.Config.create
      ~local:5
      ~redirect_stderr:`Dev_null
      ~redirect_stdout:`Dev_null
      ()
  in
  let%bind sum =
    Rpc_parallel.Map_reduce.map_reduce_commutative
      config
      (Pipe.of_list input)
      ~m:(module Count_map_reduce_function)
      ~param:multiplier
  in
  assert (
    Option.value_exn sum
    = multiplier
      * List.fold ~init:0 ~f:( + ) (List.map input ~f:(List.fold ~init:0 ~f:( + ))));
  Deferred.unit
;;

let test_map_reduce () =
  let n = 1000 in
  let input = List.init n ~f:Fn.id in
  let config =
    Rpc_parallel.Map_reduce.Config.create
      ~local:5
      ~redirect_stderr:`Dev_null
      ~redirect_stdout:`Dev_null
      ()
  in
  let%bind l =
    Rpc_parallel.Map_reduce.map_reduce
      config
      (Pipe.of_list input)
      ~m:(module Concat_map_reduce_function)
      ~param:()
  in
  assert (Option.value_exn l = input);
  Deferred.unit
;;

let close_map_wait () = Clock_ns.after (Time_ns.Span.of_sec 0.1)

let test_close_pipe_map_unordered () =
  let n = 1000 in
  let input = List.init n ~f:Fn.id in
  let config =
    Rpc_parallel.Map_reduce.Config.create
      ~local:1
      ~redirect_stderr:`Dev_null
      ~redirect_stdout:`Dev_null
      ()
  in
  let%bind output =
    Rpc_parallel.Map_reduce.map_unordered
      config
      (Pipe.of_list input)
      ~m:(module Add_map_function)
      ~param:n
  in
  match%bind Pipe.read output with
  | `Eof -> assert false
  | `Ok _ ->
    Pipe.close_read output;
    (* Give some time for the worker to read one more value from the pipe and fail (if
       there's a bug) *)
    close_map_wait ()
;;

let test_close_pipe_map () =
  let n = 1000 in
  let input = List.init n ~f:Fn.id in
  let config =
    Rpc_parallel.Map_reduce.Config.create
      ~local:1
      ~redirect_stderr:`Dev_null
      ~redirect_stdout:`Dev_null
      ()
  in
  let%bind output =
    Rpc_parallel.Map_reduce.map
      config
      (Pipe.of_list input)
      ~m:(module Add_map_function)
      ~param:n
  in
  match%bind Pipe.read output with
  | `Eof -> assert false
  | `Ok _ ->
    Pipe.close_read output;
    (* Give some time for the worker to read one more value from the pipe and fail (if
       there's a bug) *)
    close_map_wait ()
;;

let tests =
  [ "map_unordered", test_map_unordered
  ; "map", test_map
  ; "map_reduce_commutative", test_map_reduce_commutative
  ; "map_reduce", test_map_reduce
  ; "close_pipe_map_unordered", test_close_pipe_map_unordered
  ; "close_pipe_map", test_close_pipe_map
  ]
;;

let () =
  Rpc_parallel_krb_public.start_app
    ~krb_mode:For_unit_test
    (Command.basic_spec Command.Spec.empty ~summary:"Run tests" (fun () ->
       Runner.main ~check_fds:false tests))
;;
