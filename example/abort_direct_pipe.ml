open! Core
open! Async

module Worker = struct
  module T = struct
    type 'worker functions =
      { ping : ('worker, int, unit) Rpc_parallel.Function.t
      ; pongs : ('worker, unit, int) Rpc_parallel.Function.Direct_pipe.t
      ; pongs_closed : ('worker, unit, unit) Rpc_parallel.Function.t
      }

    module Worker_state = struct
      type t =
        { ping : (int, read_write) Mvar.t
        ; pongs_closed : unit Ivar.t
        }

      type init_arg = unit [@@deriving bin_io]
    end

    module Connection_state = struct
      type t = unit
      type init_arg = unit [@@deriving bin_io]
    end

    module Functions
        (C : Rpc_parallel.Creator
             with type worker_state := Worker_state.t
              and type connection_state := Connection_state.t) =
    struct
      let init_worker_state () =
        return { Worker_state.ping = Mvar.create (); pongs_closed = Ivar.create () }
      ;;

      let init_connection_state
        ~connection:(_ : Rpc.Connection.t)
        ~worker_state:(_ : Worker_state.t)
        ()
        =
        return ()
      ;;

      let functions =
        { ping =
            C.create_rpc
              ~f:(fun ~worker_state:{ ping; pongs_closed = _ } ~conn_state:() i ->
                Mvar.put ping i)
              ~bin_input:[%bin_type_class: int]
              ~bin_output:[%bin_type_class: unit]
              ()
        ; pongs =
            C.create_direct_pipe
              ~f:(fun ~worker_state:{ ping; pongs_closed } ~conn_state:() () writer ->
                Deferred.repeat_until_finished () (fun () ->
                  let module Writer = Rpc.Pipe_rpc.Direct_stream_writer in
                  match%map
                    choose
                      [ choice (Mvar.take ping) (Writer.write_without_pushback writer)
                      ; choice (Writer.closed writer) (fun () -> `Closed)
                      ]
                  with
                  | `Ok -> `Repeat ()
                  | `Closed -> `Finished ())
                >>> Ivar.fill_exn pongs_closed;
                return ())
              ~bin_input:[%bin_type_class: unit]
              ~bin_output:[%bin_type_class: int]
              ()
        ; pongs_closed =
            C.create_rpc
              ~f:(fun ~worker_state:{ ping = _; pongs_closed } ~conn_state:() () ->
                Ivar.read pongs_closed)
              ~bin_input:[%bin_type_class: unit]
              ~bin_output:[%bin_type_class: unit]
              ()
        }
      ;;
    end
  end

  include T
  include Rpc_parallel.Make (T)
end

let command =
  Command.async
    ~summary:"Abort a direct pipe rpc"
    (let%map_open.Command n =
       flag_optional_with_default_doc_sexp
         "n"
         int
         [%sexp_of: int]
         ~default:5
         ~doc:"INT number of pings to send"
     in
     fun () ->
       assert (n > 0);
       let%bind connection =
         Worker.spawn_exn
           ~on_failure:Error.raise
           ~shutdown_on:Connection_closed
           ~redirect_stdout:`Dev_null
           ~redirect_stderr:`Dev_null
           ()
           ~connection_state_init_arg:()
       in
       let ping i =
         printf "Ping: %d\n" i;
         Worker.Connection.run_exn connection ~f:Worker.functions.ping ~arg:i
       in
       let the_id = Set_once.create () in
       let closed = Ivar.create () in
       let%bind id =
         Worker.Connection.run_exn
           connection
           ~f:Worker.functions.pongs
           ~arg:
             ( ()
             , fun message ->
                 (match message with
                  | Update i ->
                    printf "Pong: %d\n" i;
                    let i = succ i in
                    if i < n
                    then don't_wait_for (ping i)
                    else Worker.Connection.abort connection ~id:(Set_once.get_exn the_id)
                  | Closed reason -> Ivar.fill_exn closed reason);
                 Continue )
       in
       Set_once.set_exn the_id id;
       let%bind () = ping 0 in
       let%bind reason = Ivar.read closed in
       printf !"Closed: %{sexp: [ `By_remote_side | `Error of Error.t ]}\n" reason;
       let%bind () =
         Worker.Connection.run_exn connection ~f:Worker.functions.pongs_closed ~arg:()
       in
       printf "Worker reports pongs closed\n";
       Worker.Connection.close connection)
    ~behave_nicely_in_pipeline:false
;;

let () = Rpc_parallel_krb_public.start_app ~krb_mode:For_unit_test command
