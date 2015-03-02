open Core.Std
open Async.Std
open Rpc_parallel_core.Std

(* Hack for ocamlbuild *)
module Parallel = Rpc_parallel_core.Std.Parallel

module Fd_redirection = Parallel.Fd_redirection

(* Like [Monitor.try_with], but raise any additional exceptions to the specified
   monitor. *)
let try_within ~monitor f =
  let ivar = Ivar.create () in
  Scheduler.within ~monitor (fun () ->
    Monitor.try_with ~extract_exn:true ~rest:`Raise f
    >>> fun r ->
    Ivar.fill ivar (Result.map_error r ~f:Error.of_exn)
  );
  Ivar.read ivar
;;

module Function = struct
  module Function_piped = struct
    type ('worker, 'query, 'response) t = ('query, 'response, Error.t) Rpc.Pipe_rpc.t

    module Id = Unique_id.Int()

    let make_impl ~monitor ~f protocol =
      Rpc.Pipe_rpc.implement protocol
        (fun () arg ~aborted:_ ->
           (* If [f] fails before returning, we want to pass the exception on so that the
              caller sees it, but if we raise after [f] has already returned or failed, we
              want [monitor] to handle it. *)
           try_within ~monitor (fun () -> f arg))
    ;;

    let make_proto ?name ~bin_input ~bin_output () =
      let name = match name with
        | None -> sprintf "proto-stream%s" (Id.to_string (Id.create ()))
        | Some n -> n
      in
      Rpc.Pipe_rpc.create
        ~name
        ~version:0
        ~bin_query:bin_input
        ~bin_response:bin_output
        ~bin_error:Error.bin_t
        ()
  end

  module Function_plain = struct
    type ('worker, 'query, 'response) t = ('query, 'response) Rpc.Rpc.t

    module Id = Unique_id.Int()

    let make_impl ~monitor ~f protocol =
      Rpc.Rpc.implement protocol (fun () arg ->
        (* If [f] raises an exception before returning, we want this whole implementation
           to fail so that the caller sees it. If [f] raises after returning, we want
           [monitor] to handle it. *)
        try_within ~monitor (fun () -> f arg)
        >>| function
        | Ok x -> x
        | Error error -> Error.raise error)
    ;;

    let make_proto ?name ~bin_input ~bin_output () =
      let name = match name with
        | None -> sprintf "proto%s" (Id.to_string (Id.create ()))
        | Some n -> n
      in
      Rpc.Rpc.create
        ~name
        ~version:0
        ~bin_query:bin_input
        ~bin_response:bin_output
  end

  type ('worker, 'query, 'response) t =
    | Plain of ('worker, 'query, 'response) Function_plain.t
    | Piped
      :  ('worker, 'query, 'response) Function_piped.t
      *  ('r, 'response Pipe.Reader.t) Type_equal.t
      -> ('worker, 'query, 'r) t

  let create_rpc ~implementations ~monitor ?name ~f ~bin_input ~bin_output () =
    let proto = Function_plain.make_proto ?name ~bin_input ~bin_output () in
    implementations := (Function_plain.make_impl ~monitor ~f proto) :: !implementations;
    Plain proto

  let create_pipe ~implementations ~monitor ?name ~f ~bin_input ~bin_output () =
    let proto = Function_piped.make_proto ?name ~bin_input ~bin_output () in
    implementations := (Function_piped.make_impl ~monitor ~f proto) :: !implementations;
    Piped (proto, Type_equal.T)

  let of_async_rpc ~implementations ~monitor ~f proto =
    implementations := (Function_plain.make_impl ~monitor ~f proto) :: !implementations;
    Plain proto

  let of_async_pipe_rpc ~implementations ~monitor ~f proto =
    implementations := (Function_piped.make_impl ~monitor ~f proto) :: !implementations;
    Piped (proto, Type_equal.T)

  let run (type response) (t : (_, _, response) t) connection ~arg
    : response Or_error.t Deferred.t =
    match t with
    | Plain proto -> Rpc.Rpc.dispatch proto connection arg
    | Piped (proto, Type_equal.T) ->
      Rpc.Pipe_rpc.dispatch proto connection arg
      >>| fun result ->
      Or_error.join result
      |> Or_error.map ~f:(fun (reader, _) -> reader)
end

module type Creator = sig
  type worker with bin_io
  type state

  val create_rpc :
    ?name: string
    -> f: (state -> 'query -> 'response Deferred.t)
    -> bin_input: 'query Bin_prot.Type_class.t
    -> bin_output: 'response Bin_prot.Type_class.t
    -> unit
    -> (worker, 'query, 'response) Function.t

  val create_pipe :
    ?name: string
    -> f: (state -> 'query -> 'response Pipe.Reader.t Deferred.t)
    -> bin_input: 'query Bin_prot.Type_class.t
    -> bin_output: 'response Bin_prot.Type_class.t
    -> unit
    -> (worker, 'query, 'response Pipe.Reader.t) Function.t

  val of_async_rpc :
    f: (state -> 'query -> 'response Deferred.t)
    -> ('query, 'response) Rpc.Rpc.t
    -> (worker, 'query, 'response) Function.t

  val of_async_pipe_rpc :
    f: (state -> 'query -> 'response Pipe.Reader.t Deferred.t)
    -> ('query, 'response, Error.t) Rpc.Pipe_rpc.t
    -> (worker, 'query, 'response Pipe.Reader.t) Function.t

  val run :
    worker
    -> f: (worker, 'query, 'response) Function.t
    -> arg: 'query
    -> 'response Or_error.t Deferred.t

  val run_exn :
    worker
    -> f: (worker, 'query, 'response) Function.t
    -> arg: 'query
    -> 'response Deferred.t
end

module type Functions = sig
  type worker
  type 'worker functions
  val functions : worker functions
end

(* Generate unique identifiers for each application of the [Make_worker] functor *)
module Id = Unique_id.Int()

(* Table from [Id.t] to [unit -> (Socket.Address.Inet.t, int) Tcp.Server.t] *)
let serve_functions = Hashtbl.Poly.create ()

(* Table from worker [Host_and_port.t]'s to [conn] *)
type conn = Connected of Rpc.Connection.t | Pending of Rpc.Connection.t Or_error.t Ivar.t
let connections = Hashtbl.Poly.create ()

(* Find the implementations for the given worker type (determined by the [Id.t] given to
   it at the top level of the [Make_worker] functor application), host an Rpc server with
   these implementations and return back a [Host_and_port.t] describing the server *)
let worker_main id =
  let serve = Hashtbl.find_exn serve_functions id in
  serve ()
  >>| fun serv ->
  Host_and_port.create ~host:(Unix.gethostname()) ~port:(Tcp.Server.listening_on serv)

module Parallel_core = Parallel.Make(struct
  type worker_arg = Id.t with bin_io
  type worker_ret = Host_and_port.t with bin_io
  let worker_main = worker_main
end)

(* Table from worker [Host_and_port.t]'s to [Parallel_core.worker_id] *)
let internal_workers = Hashtbl.Poly.create ()

module Remote_executable = Parallel.Remote_executable

module type Worker = sig
  type t with bin_io
  type 'a functions
  type init_arg

  val functions : t functions

  val spawn
    :  ?where : [`Local | `Remote of _ Remote_executable.t]
    -> ?disown : bool
    -> ?env : (string * string) list
    -> ?redirect_stdout : Fd_redirection.t
    -> ?redirect_stderr : Fd_redirection.t
    -> ?cd : string
    -> ?umask : int
    -> init_arg
    -> on_failure : (Error.t -> unit)
    -> t Or_error.t Deferred.t

  val spawn_exn
    :  ?where : [`Local | `Remote of _ Remote_executable.t]
    -> ?disown : bool
    -> ?env : (string * string) list
    -> ?redirect_stdout : Fd_redirection.t
    -> ?redirect_stderr : Fd_redirection.t
    -> ?cd : string
    -> ?umask : int
    -> init_arg
    -> on_failure : (Error.t -> unit)
    -> t Deferred.t

  val run :
    t
    -> f: (t, 'query, 'response) Function.t
    -> arg: 'query
    -> 'response Or_error.t Deferred.t

  val run_exn :
    t
    -> f: (t, 'query, 'response) Function.t
    -> arg: 'query
    -> 'response Deferred.t

  val disconnect : t -> unit Deferred.t

  val kill : t -> unit Deferred.t

  val host_and_port : t -> Host_and_port.t
end

module type Worker_spec = sig
  type 'worker functions

  type init_arg with bin_io
  type state
  val init : init_arg -> state Deferred.t

  module Functions(C:Creator with type state := state) : Functions
    with type 'a functions := 'a functions
    with type worker := C.worker
end

module Make_worker(S:Worker_spec) = struct
  type t = Host_and_port.t with bin_io

  (* A unique identifier for each application of the [Make_worker] functor. Because we are
     running the same executable, the master and the workers will agree on these ids *)
  let id = Id.create()

  let worker_init_rpc =
    Rpc.Rpc.create
      ~name:"worker_init_rpc"
      ~version:0
      ~bin_query:S.bin_init_arg
      ~bin_response:(Or_error.bin_t Unit.bin_t)

  let worker_exn_rpc =
    Rpc.Pipe_rpc.create
      ~name:"worker_exn_rpc"
      ~version:0
      ~bin_query:Unit.bin_t
      ~bin_response:Error.bin_t
      ~bin_error:Error.bin_t
      ()


  let monitor = Monitor.create ()
  let implementations = ref []
  let worker_state = Set_once.create ()

  (* Add extra implementations for worker_init_rpc and worker_exn_rpc *)
  let () =
    let worker_init_impl =
      Rpc.Rpc.implement worker_init_rpc (fun () arg ->
        let ivar = Ivar.create () in
        Scheduler.within ~monitor (fun () ->
          Monitor.try_with ~rest:`Raise (fun () ->
            S.init arg
            >>| fun state ->
            Set_once.set_exn worker_state state)
          >>> fun result ->
          Ivar.fill ivar (Or_error.of_exn_result result));
        Ivar.read ivar)
    in
    let worker_exn_impl =
      Rpc.Pipe_rpc.implement worker_exn_rpc (fun _state () ~aborted ->
        let _ = aborted in
        let r, w = Pipe.create () in
        Monitor.detach_and_iter_errors monitor ~f:(fun exn ->
          Pipe.write w (Error.of_exn exn)
          >>> fun () ->
          raise exn);
        return (Ok r))
    in
    implementations := worker_init_impl :: worker_exn_impl :: !implementations;
    Hashtbl.add_exn serve_functions ~key:id ~data:(fun () ->
      let rpc_implementations =
        Rpc.Implementations.create_exn
          ~implementations:!implementations
          ~on_unknown_rpc:`Raise
      in
      Rpc.Connection.serve ~implementations:rpc_implementations
        ~initial_connection_state:(fun _ _ -> ())
        ~where_to_listen:Tcp.on_port_chosen_by_os ()
    )

  let store_connection worker connection =
    Hashtbl.replace connections ~key:worker ~data:(Connected connection);
    Rpc.Connection.close_finished connection
    >>> fun () ->
    Hashtbl.remove connections worker

  let get_connection worker =
    match Hashtbl.find connections worker with
    | None ->
      (* Make a new connection and store it *)
      let ivar = Ivar.create () in
      Hashtbl.add_exn connections ~key:worker ~data:(Pending ivar);
      (Rpc.Connection.client
         ~host:(Host_and_port.host worker)
         ~port:(Host_and_port.port worker)
         ()
       >>| function
       | Error e ->
         Hashtbl.remove connections worker;
         let err = Error (Error.of_exn e) in
         Ivar.fill ivar err; err
       | Ok conn ->
         Ivar.fill ivar (Ok conn);
         store_connection worker conn; Ok conn)
    | Some (Connected conn) -> return (Ok conn)
    | Some (Pending ivar) -> Ivar.read ivar

  let setup_initial_state ~worker arg ~on_failure =
    Rpc.Connection.client
      ~host:(Host_and_port.host worker)
      ~port:(Host_and_port.port worker)
      ()
    >>= function
    | Error e -> return (Error (Error.of_exn e))
    | Ok conn ->
      Rpc.Pipe_rpc.dispatch worker_exn_rpc conn ()
      >>| Or_error.join
      >>= function
      | Error _ as e ->
        Rpc.Connection.close conn
        >>| fun () ->
        e
      | Ok (reader, _) ->
        don't_wait_for (
          Pipe.iter reader ~f:(fun err -> return (on_failure err))
        );
        Rpc.Rpc.dispatch worker_init_rpc conn arg
        >>| Or_error.join
        >>= function
        | Error _ as e ->
          Rpc.Connection.close conn
          >>| fun () ->
          e
        | Ok () ->
          store_connection worker conn;
          return (Ok worker)

  let spawn ?where ?disown ?env
        ?redirect_stdout ?redirect_stderr ?cd ?umask
        arg ~on_failure =
    Parallel_core.spawn_worker ?where ?disown ?env
      ?redirect_stdout ?redirect_stderr ?cd ?umask
      id ~on_failure
    >>= function
    | Error e -> return (Error e)
    | Ok (t, id) ->
      Hashtbl.add_exn internal_workers ~key:t ~data:id;
      setup_initial_state ~worker:t arg ~on_failure

  let spawn_exn ?where ?disown ?env
        ?redirect_stdout ?redirect_stderr ?cd ?umask
        arg ~on_failure =
    spawn ?where ?disown ?env
      ?redirect_stdout ?redirect_stderr ?cd ?umask
      arg ~on_failure
    >>| Or_error.ok_exn

  let run t ~f ~arg =
    get_connection t
    >>= function
    | Error e -> return (Error e)
    | Ok conn -> Function.run f conn ~arg

  let run_exn t ~f ~arg  = run t ~f ~arg >>| Or_error.ok_exn

  let host_and_port t = t

  let disconnect t =
    match Hashtbl.find connections t with
    | None -> return ()
    | Some (Connected conn) ->
      Rpc.Connection.close conn
    | Some (Pending ivar) ->
      Ivar.read ivar
      >>= function
      | Error _ -> return ()
      | Ok conn -> Rpc.Connection.close conn

  let kill t =
    match Hashtbl.find internal_workers t with
    | None -> return ()
    | Some id ->
      Hashtbl.remove internal_workers t;
      Parallel_core.kill_worker id


  module Function_creator = struct
    type worker = t with bin_io

    let wrap f x = f (Set_once.get_exn worker_state) x

    let create_rpc ?name ~f ~bin_input ~bin_output () =
      Function.create_rpc ~implementations ~monitor ?name ~f:(wrap f)
        ~bin_input ~bin_output ()

    let create_pipe ?name ~f ~bin_input ~bin_output () =
      Function.create_pipe ~implementations ~monitor ?name ~f:(wrap f)
        ~bin_input ~bin_output ()

    let of_async_rpc ~f proto =
      Function.of_async_rpc ~implementations ~monitor ~f:(wrap f) proto

    let of_async_pipe_rpc ~f proto =
      Function.of_async_pipe_rpc ~implementations ~monitor ~f:(wrap f) proto

    let run = run
    let run_exn = run_exn
  end

  module User_functions = S.Functions(Function_creator)

  let functions = User_functions.functions

end

module State = struct
  type t = [ `started ]

  let t_opt = ref None
  ;;

  let get () = !t_opt
  ;;

  let set_is_started () = t_opt := Some `started
  ;;

end

let start_app command =
  State.set_is_started ();
  Parallel_core.run command
;;
