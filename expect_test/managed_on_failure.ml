open Core
open Async

let () = Dynamic.set_root Backtrace.elide true

module T = struct
  type 'worker functions = { fail : ('worker, unit, unit) Rpc_parallel.Function.t }

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
    let fail =
      C.create_one_way
        ~f:(fun ~worker_state:() ~conn_state:() () ->
          (* Make sure this exception is raised asynchronously. I'm not sure how to do
             this in a non-racy way, but hopefully 0.01 seconds strikes the right balance
             of not being racy but not introducing too much of a delay. *)
          upon (after (sec 0.01)) (fun () -> failwith "asynchronous exception"))
        ~bin_input:Unit.bin_t
        ()
    ;;

    let functions = { fail }
    let init_worker_state () = Deferred.unit
    let init_connection_state ~connection:_ ~worker_state:_ = return
  end
end

include Rpc_parallel.Managed.Make [@alert "-legacy"] (T)

let uuid_re =
  Re.Pcre.re "[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}" |> Re.compile
;;

let uuid_replacement = Uuid.Stable.V1.for_testing |> Uuid.to_string

let error_to_string_masking_uuid error =
  Re.replace_string uuid_re ~by:uuid_replacement (Error.to_string_hum error)
;;

let main () =
  let errors = Transaction.Var.create [] in
  let add_error ~tag error =
    Transaction.Var.replace_now errors (fun errors -> Error.tag ~tag error :: errors)
  in
  let%bind worker =
    spawn
      ~on_failure:(add_error ~tag:"on_failure")
      ~on_connection_to_worker_closed:(add_error ~tag:"on_connection_to_worker_closed")
      ~redirect_stdout:`Dev_null
      ~redirect_stderr:`Dev_null
      ()
      ()
    >>| ok_exn
  in
  let%bind () = run_exn worker ~f:functions.fail ~arg:() in
  match%bind
    (let open Transaction.Let_syntax in
     match%bind Transaction.Var.get errors with
     | _ :: _ :: _ as errors -> return errors
     | _ -> Transaction.retry ())
    |> Transaction.run_with_timeout (Time_ns.Span.of_sec 10.)
  with
  | Result errors ->
    let errors =
      errors
      |> List.map ~f:error_to_string_masking_uuid
      |> List.sort ~compare:String.compare
    in
    print_s [%message (errors : string list)];
    return ()
  | Timeout () ->
    print_s [%message "Timeout"];
    return ()
;;

let () = Rpc_parallel_krb_public.For_testing.initialize [%here]

let%expect_test "" =
  let%bind () = main () in
  [%expect
    {|
    (errors
     ("(on_connection_to_worker_closed \"Lost connection with worker\")"
       "(on_failure\
      \n (5a863fc1-67b7-3a0a-dc90-aca2995afbf9\
      \n  (monitor.ml.Error (Failure \"asynchronous exception\")\
      \n   (\"<backtrace elided in test>\"))))"))
    |}];
  return ()
;;
