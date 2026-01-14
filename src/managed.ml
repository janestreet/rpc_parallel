open Core
open Async

module type Worker = sig
  type t [@@deriving sexp_of]
  type unmanaged_t
  type 'a functions

  val functions : unmanaged_t functions

  type worker_state_init_arg
  type connection_state_init_arg

  module Id : Identifiable

  val id : t -> Id.t

  val spawn
    :  ?how:How_to_run.t
    -> ?name:string
    -> ?env:(string * string) list
    -> ?connection_timeout:Time_float.Span.t
    -> ?cd:string
    -> ?umask:int
    -> redirect_stdout:Fd_redirection.t
    -> redirect_stderr:Fd_redirection.t
    -> worker_state_init_arg
    -> connection_state_init_arg
    -> on_failure:(Error.t -> unit)
    -> on_connection_to_worker_closed:(Error.t -> unit)
    -> t Or_error.t Deferred.t

  val spawn_exn
    :  ?how:How_to_run.t
    -> ?name:string
    -> ?env:(string * string) list
    -> ?connection_timeout:Time_float.Span.t
    -> ?cd:string
    -> ?umask:int
    -> redirect_stdout:Fd_redirection.t
    -> redirect_stderr:Fd_redirection.t
    -> worker_state_init_arg
    -> connection_state_init_arg
    -> on_failure:(Error.t -> unit)
    -> on_connection_to_worker_closed:(Error.t -> unit)
    -> t Deferred.t

  val run
    :  ?on_pipe_rpc_close_error:(Error.t -> unit)
    -> t
    -> f:(unmanaged_t, 'query, 'response) Parallel.Function.t
    -> arg:'query
    -> 'response Or_error.t Deferred.t

  val run_exn
    :  ?on_pipe_rpc_close_error:(Error.t -> unit)
    -> t
    -> f:(unmanaged_t, 'query, 'response) Parallel.Function.t
    -> arg:'query
    -> 'response Deferred.t

  val kill : t -> unit Or_error.t Deferred.t
  val kill_exn : t -> unit Deferred.t
end

module Make (S : Parallel.Worker_spec) = struct
  module Unmanaged = Parallel.Make (S)
  module Id = Utils.Worker_id

  type nonrec t =
    { unmanaged : Unmanaged.t
    ; connection_state_init_arg : S.Connection_state.init_arg
    ; id : Id.t
    }

  type unmanaged_t = Unmanaged.t

  type conn =
    [ `Pending of Unmanaged.Connection.t Or_error.t Ivar.t
    | `Connected of Unmanaged.Connection.t
    ]

  let sexp_of_t t = [%sexp_of: Unmanaged.t] t.unmanaged
  let id t = t.id
  let functions = Unmanaged.functions
  let workers : conn Id.Table.t = Id.Table.create ()

  let get_connection { unmanaged = t; connection_state_init_arg; id } =
    match Hashtbl.find workers id with
    | Some (`Pending ivar) -> Ivar.read ivar
    | Some (`Connected conn) -> Deferred.Or_error.return conn
    | None ->
      let ivar = Ivar.create () in
      Hashtbl.add_exn workers ~key:id ~data:(`Pending ivar);
      (match%map Unmanaged.Connection.client t connection_state_init_arg with
       | Error e ->
         Ivar.fill_exn ivar (Error e);
         Hashtbl.remove workers id;
         Error e
       | Ok conn ->
         Ivar.fill_exn ivar (Ok conn);
         Hashtbl.set workers ~key:id ~data:(`Connected conn);
         (Unmanaged.Connection.close_finished conn >>> fun () -> Hashtbl.remove workers id);
         Ok conn)
  ;;

  let with_shutdown_on_error worker ~f =
    match%bind f () with
    | Ok _ as ret -> return ret
    | Error _ as ret ->
      let%bind (_ : unit Or_error.t) = Unmanaged.shutdown worker in
      return ret
  ;;

  let spawn
    ?how
    ?name
    ?env
    ?connection_timeout
    ?cd
    ?umask
    ~redirect_stdout
    ~redirect_stderr
    worker_state_init_arg
    connection_state_init_arg
    ~on_failure
    ~on_connection_to_worker_closed
    =
    Unmanaged.spawn
      ?how
      ?env
      ?name
      ?connection_timeout
      ?cd
      ?umask
      ~shutdown_on:Heartbeater_connection_timeout
      ~redirect_stdout
      ~redirect_stderr
      worker_state_init_arg
      ~on_failure
    >>=? fun worker ->
    with_shutdown_on_error worker ~f:(fun () ->
      Unmanaged.Connection.client worker connection_state_init_arg)
    >>|? fun connection ->
    let id = Id.create () in
    Hashtbl.add_exn workers ~key:id ~data:(`Connected connection);
    (Unmanaged.Connection.close_finished connection
     >>> fun () ->
     match Hashtbl.find workers id with
     | None ->
       (* [kill] was called, don't report closed connection *)
       ()
     | Some _ ->
       Hashtbl.remove workers id;
       let error = Error.createf !"Lost connection with worker" in
       on_connection_to_worker_closed error);
    { unmanaged = worker; connection_state_init_arg; id }
  ;;

  let spawn_exn
    ?how
    ?name
    ?env
    ?connection_timeout
    ?cd
    ?umask
    ~redirect_stdout
    ~redirect_stderr
    worker_state_init_arg
    connection_init_arg
    ~on_failure
    ~on_connection_to_worker_closed
    =
    spawn
      ?how
      ?name
      ?env
      ?connection_timeout
      ?cd
      ?umask
      ~redirect_stdout
      ~redirect_stderr
      worker_state_init_arg
      connection_init_arg
      ~on_failure
      ~on_connection_to_worker_closed
    >>| Or_error.ok_exn
  ;;

  let kill t =
    Hashtbl.remove workers t.id;
    Unmanaged.shutdown t.unmanaged
  ;;

  let kill_exn t = kill t >>| Or_error.ok_exn

  let run ?on_pipe_rpc_close_error t ~f ~arg =
    get_connection t
    >>=? fun conn -> Unmanaged.Connection.run ?on_pipe_rpc_close_error conn ~f ~arg
  ;;

  let run_exn ?on_pipe_rpc_close_error t ~f ~arg =
    run ?on_pipe_rpc_close_error t ~f ~arg >>| Or_error.ok_exn
  ;;
end
