open! Core
open Async

module Pid_and_host = struct
  type t =
    { pid : Pid.t
    ; host : string
    }
  [@@deriving bin_io, compare, sexp_of]

  let get_mine () = { pid = Unix.getpid (); host = Unix.gethostname () }

  let get_master () =
    let open Option.Let_syntax in
    let%bind pid_and_host =
      Array.find_map (Sys.get_argv ()) ~f:(String.chop_prefix ~prefix:"child-of-")
    in
    let%map pid, host = String.lsplit2 pid_and_host ~on:'@' in
    { pid = Pid.of_string pid; host }
  ;;
end

module Master_and_worker = struct
  type t =
    { master : Pid_and_host.t option
    ; worker : Pid_and_host.t
    }
  [@@deriving bin_io, compare, sexp_of]

  let get () = { master = Pid_and_host.get_master (); worker = Pid_and_host.get_mine () }
end

module rec Worker : sig
  type t

  val spawn : children_to_spawn:int -> t Deferred.t
  val get_master_and_worker_chain : t -> Master_and_worker.t list Deferred.t
end = struct
  module Impl = struct
    type 'w functions =
      { get_master_and_worker_chain :
          ('w, unit, Master_and_worker.t list) Rpc_parallel.Function.t
      }

    module Worker_state = struct
      type init_arg = { children_to_spawn : int } [@@deriving bin_io]
      type t = { child : Worker.t option }
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
      let functions =
        { get_master_and_worker_chain =
            C.create_rpc
              ~bin_input:[%bin_type_class: unit]
              ~bin_output:[%bin_type_class: Master_and_worker.t list]
              ~f:(fun ~worker_state:{ child } ~conn_state:() () ->
                let%map tl =
                  match child with
                  | None -> return []
                  | Some child -> Worker.get_master_and_worker_chain child
                in
                Master_and_worker.get () :: tl)
              ()
        }
      ;;

      let init_worker_state { Worker_state.children_to_spawn } =
        if children_to_spawn > 0
        then (
          let%map child = Worker.spawn ~children_to_spawn:(pred children_to_spawn) in
          { Worker_state.child = Some child })
        else return { Worker_state.child = None }
      ;;

      let init_connection_state
        ~connection:(_ : Rpc.Connection.t)
        ~worker_state:(_ : Worker_state.t)
        ()
        =
        return ()
      ;;
    end
  end

  module Rpc_parallel_worker = Rpc_parallel.Make (Impl)

  type t = Rpc_parallel_worker.Connection.t

  let spawn ~children_to_spawn =
    Rpc_parallel_worker.spawn_exn
      ~on_failure:Error.raise
      ~shutdown_on:Connection_closed
      ~redirect_stdout:`Dev_null
      ~redirect_stderr:`Dev_null
      ~connection_state_init_arg:()
      { children_to_spawn }
  ;;

  let get_master_and_worker_chain t =
    Rpc_parallel_worker.Connection.run_exn
      t
      ~f:Rpc_parallel_worker.functions.get_master_and_worker_chain
      ~arg:()
  ;;
end

let () = Rpc_parallel_krb_public.For_testing.initialize [%here]

let%expect_test _ =
  let%bind worker = Worker.spawn ~children_to_spawn:10 in
  let%map chain = Worker.get_master_and_worker_chain worker in
  let chain = Master_and_worker.get () :: chain in
  let masters, workers =
    List.unzip (List.map chain ~f:(fun { master; worker } -> master, worker))
  in
  [%test_result: Pid_and_host.t option list]
    (None :: List.map (List.drop_last_exn workers) ~f:Option.some)
    ~expect:masters
;;
