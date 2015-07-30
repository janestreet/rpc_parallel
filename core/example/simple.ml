open Core.Std
open Async.Std
open Rpc_parallel_core.Std

(* A simple use of the [Rpc_parallel_core] library. Spawn two different types of
   workers, one that can compute sums and another that can compute products. Give work to
   both of them and print the results. *)

(* Sum Worker *)
let sum_rpc =
  Rpc.Rpc.create
    ~name:"sum_rpc"
    ~version:0
    ~bin_query:Int.bin_t
    ~bin_response:Int.bin_t

let sum_rpc_impl =
  Rpc.Rpc.implement sum_rpc (fun () to_ ->
    let sum = List.fold ~init:0 ~f:(+) (List.init to_ ~f:Fn.id) in
    printf "sum: %i\n" sum;
    return sum)

let sum_implementations =
  Rpc.Implementations.create_exn ~implementations:[sum_rpc_impl]
    ~on_unknown_rpc:`Close_connection

(* Prod Worker *)
let prod_rpc =
  Rpc.Rpc.create
    ~name:"prod_rpc"
    ~version:0
    ~bin_query:Int.bin_t
    ~bin_response:Int.bin_t

let prod_rpc_impl =
  Rpc.Rpc.implement prod_rpc (fun () to_ ->
    let prod = List.fold ~init:1 ~f:( * ) (List.init (to_-1) ~f:(fun i -> i+1)) in
    printf "prod: %i\n" prod;
    return prod)

let prod_implementations =
  Rpc.Implementations.create_exn ~implementations:[prod_rpc_impl]
    ~on_unknown_rpc:`Close_connection

let worker_main ?rpc_max_message_size ?rpc_handshake_timeout ?rpc_heartbeat_config
      op =
  let implementations = (match op with
  | `Sum -> sum_implementations
  | `Product -> prod_implementations)
  in
  Rpc.Connection.serve ~implementations
    ?max_message_size:rpc_max_message_size
    ?handshake_timeout:rpc_handshake_timeout
    ?heartbeat_config:rpc_heartbeat_config
    ~initial_connection_state:(fun _ _ -> ())
    ~where_to_listen:Tcp.on_port_chosen_by_os ()
  >>| fun serv ->
  Host_and_port.create ~host:(Unix.gethostname()) ~port:(Tcp.Server.listening_on serv)

module Parallel_app = Parallel.Make(struct
  type worker_arg = [`Sum | `Product] with bin_io
  type worker_ret = Host_and_port.t with bin_io
  let worker_main = worker_main
end)

let handle_error worker err =
  failwiths (sprintf "error in %s" worker) err Error.sexp_of_t

let command =
  Command.async ~summary:"foo"
    Command.Spec.(
      empty
      +> flag "max" (required int) ~doc:""
      +> flag "log-dir" (optional string)
           ~doc:" Folder to write worker logs to"
    )
    (fun max log_dir () ->
       let redirect_stdout_sum, redirect_stderr_sum,
           redirect_stdout_prod, redirect_stderr_prod =
         match log_dir with
         | None -> (`Dev_null, `Dev_null, `Dev_null, `Dev_null)
         | Some _ ->
           (`File_append "sum.out", `File_append "sum.err",
            `File_append "prod.out", `File_append "prod.err")
       in
       Parallel_app.spawn_worker_exn ~where:`Local `Sum
         ?cd:log_dir
         ~redirect_stdout:redirect_stdout_sum
         ~redirect_stderr:redirect_stderr_sum
         ~on_failure:(handle_error "sum worker") >>= fun (sum_worker, _id1) ->
       Parallel_app.spawn_worker_exn ~where:`Local `Product
         ?cd:log_dir
         ~redirect_stdout:redirect_stdout_prod
         ~redirect_stderr:redirect_stderr_prod
         ~on_failure:(handle_error "prod worker") >>= fun (prod_worker, _id2) ->
       let get_result ~rpc ~worker =
         Rpc.Connection.with_client
            ~host:(Host_and_port.host worker)
            ~port:(Host_and_port.port worker)
            (fun conn -> Rpc.Rpc.dispatch rpc conn max)
          >>| function
          | Error e -> failwiths "Connection failure" e Exn.sexp_of_t
          | Ok (Error e) -> failwiths "Dispatch failure" e Error.sexp_of_t
          | Ok (Ok i) -> i
       in
       Deferred.all [
         get_result ~rpc:prod_rpc ~worker:prod_worker;
         get_result ~rpc:sum_rpc ~worker:sum_worker
       ]
       >>= fun results ->
       Core.Std.Printf.printf "prod: %d\nsum: %d\n"
         (List.nth_exn results 0) (List.nth_exn results 1);
       return ())

let () = Parallel_app.run command
