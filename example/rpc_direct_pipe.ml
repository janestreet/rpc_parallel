open Core
open Async

module Sum_worker = struct
  module Sum_arg = struct
    type t =
      { max : int
      ; delay : Time_float.Span.t
      }
    [@@deriving bin_io]
  end

  module T = struct
    type 'worker functions =
      { sum : ('worker, Sum_arg.t, string) Rpc_parallel.Function.Direct_pipe.t }

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
      let sum_impl ~worker_state:() ~conn_state:() { Sum_arg.max; delay } writer =
        (* We make sure to write to the direct stream writer in a [don't_wait_for] because
           according to the docs at lib/async_rpc_kernel/src/rpc.mli:

           Though the implementation function is given a writer immediately, the result of
           the client's call to [dispatch] will not be determined until after the
           implementation function returns. Elements written before the function returns
           will be queued up to be written after the function returns. *)
        don't_wait_for
          (let%bind (_ : int) =
             Deferred.List.fold
               ~init:0
               ~f:(fun acc x ->
                 let acc = acc + x in
                 let output = sprintf "Sum_worker.sum: %i\n" acc in
                 let (_ : [ `Closed | `Flushed of unit Deferred.t ]) =
                   Rpc.Pipe_rpc.Direct_stream_writer.write writer output
                 in
                 let%bind () = after delay in
                 return acc)
               (List.init max ~f:Fn.id)
           in
           Rpc.Pipe_rpc.Direct_stream_writer.close writer;
           return ());
        Deferred.unit
      ;;

      let sum =
        C.create_direct_pipe
          ~f:sum_impl
          ~bin_input:Sum_arg.bin_t
          ~bin_output:String.bin_t
          ()
      ;;

      let functions = { sum }
      let init_worker_state () = return ()
      let init_connection_state ~connection:_ ~worker_state:_ = return
    end
  end

  include Rpc_parallel.Make (T)
end

let main ~max ~delay ~log_dir =
  let open Deferred.Or_error.Let_syntax in
  let redirect_stdout, redirect_stderr =
    match log_dir with
    | None -> `Dev_null, `Dev_null
    | Some _ -> `File_append "sum.out", `File_append "sum.err"
  in
  let%bind conn =
    Sum_worker.spawn
      ~on_failure:Error.raise
      ?cd:log_dir
      ~shutdown_on:Connection_closed
      ~redirect_stdout
      ~redirect_stderr
      ~connection_state_init_arg:()
      ()
  in
  let closed_ivar = Ivar.create () in
  let on_write = function
    | Rpc.Pipe_rpc.Pipe_message.Closed _ ->
      Ivar.fill_exn closed_ivar ();
      Rpc.Pipe_rpc.Pipe_response.Continue
    | Update s ->
      (* Make sure to flush the output after each write so we can show we are getting
         responded back in a streaming fashion *)
      Core.printf "%s%!" s;
      Rpc.Pipe_rpc.Pipe_response.Continue
  in
  let%bind (_ : Sum_worker.worker Rpc_parallel.Function.Direct_pipe.Id.t) =
    Sum_worker.Connection.run
      conn
      ~f:Sum_worker.functions.sum
      ~arg:({ max; delay }, on_write)
  in
  Ivar.read closed_ivar |> Deferred.ok
;;

let command =
  Command.async_or_error
    ~summary:"Example using rpc_parallel with a direct pipe"
    (let%map_open.Command max = flag "max" (required int) ~doc:"NUM max number to sum up"
     and delay =
       flag
         "delay"
         (required Time_float_unix.Span.arg_type)
         ~doc:"SPAN delay between writes"
     and log_dir =
       flag "log-dir" (optional string) ~doc:"DIR Folder to write worker logs to"
     in
     fun () -> main ~max ~delay ~log_dir)
;;

let () = Rpc_parallel_krb_public.start_app ~krb_mode:For_unit_test command
