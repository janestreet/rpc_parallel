open Core
open Async

module T = struct
  type 'worker functions =
    { string_and_updates :
        ('worker, int, string * char Pipe.Reader.t) Rpc_parallel.Function.t
    }

  module Worker_state = struct
    type init_arg = unit [@@deriving bin_io]
    type t = unit
  end

  module Connection_state = struct
    type init_arg = unit [@@deriving bin_io]
    type t = unit
  end

  module Functions
      (C : Rpc_parallel.Creator
           with type worker_state := Worker_state.t
            and type connection_state := Connection_state.t) =
  struct
    let string_and_updates_impl ~worker_state:() ~conn_state:() arg =
      let string = "hello world " in
      let chars = List.init arg ~f:(fun i -> Char.of_int_exn (Char.to_int 'a' + i)) in
      return (string, Pipe.of_list chars)
    ;;

    let string_and_updates =
      C.create_state
        ~f:string_and_updates_impl
        ~bin_query:Int.bin_t
        ~bin_state:String.bin_t
        ~bin_update:Char.bin_t
        ()
    ;;

    let functions = { string_and_updates }
    let init_worker_state () = Deferred.unit
    let init_connection_state ~connection:_ ~worker_state:_ = return
  end
end

include Rpc_parallel.Make (T)

let main num_chars =
  let%bind conn =
    spawn
      ~on_failure:Error.raise
      ~shutdown_on:Connection_closed
      ~redirect_stdout:`Dev_null
      ~redirect_stderr:`Dev_null
      ~connection_state_init_arg:()
      ()
    >>| ok_exn
  in
  Connection.run_exn conn ~f:functions.string_and_updates ~arg:num_chars
;;

let () = Rpc_parallel_krb_public.For_testing.initialize [%here]

let%expect_test "" =
  let%bind state, chars = main 10 in
  let%bind all_chars = Pipe.read_all chars >>| Queue.to_list in
  let result = state ^ String.of_char_list all_chars in
  printf "%s\n" result;
  [%expect {| hello world abcdefghij |}];
  return ()
;;
