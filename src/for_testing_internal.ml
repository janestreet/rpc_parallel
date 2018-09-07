open! Core
open Async

let env_var = "RPC_PARALLEL_EXPECT_TEST_INITIALIZE"
let initialize_source_code_position = ref None

let set_initialize_source_code_position here =
  initialize_source_code_position := Some (Source_code_position.to_string here)
;;

let worker_environment () =
  match !initialize_source_code_position with
  | None -> []
  | Some here -> [ env_var, here ]
;;

let worker_should_initialize here =
  match Unix.getenv env_var with
  | None -> false
  | Some env -> String.equal (Source_code_position.to_string here) env
;;
