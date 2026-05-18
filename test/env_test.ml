open Core
open Async

module Pre_worker = struct
  type 'w functions = { getenv : ('w, string, string option) Rpc_parallel.Function.t }

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
    let getenv =
      C.create_rpc
        ~bin_input:String.bin_t
        ~bin_output:(Option.bin_t String.bin_t)
        ~f:(fun ~worker_state:() ~conn_state:() key -> return (Unix.getenv key))
        ()
    ;;

    let functions = { getenv }
    let init_worker_state () = Deferred.unit
    let init_connection_state ~connection:_ ~worker_state:_ = return
  end
end

module Worker = Rpc_parallel.Make (Pre_worker)

let spawn ?env ~host () =
  let%bind how, cleanup_fn =
    match host with
    | None -> return (Rpc_parallel.How_to_run.local, fun () -> return ())
    | Some host ->
      let executable_dir = Filename.temp_dir_name in
      let%map remote_executable =
        Rpc_parallel.Remote_executable.copy_to_host
          ~strict_host_key_checking:`No
          ~executable_dir
          host
        >>| ok_exn
      in
      ( Rpc_parallel.How_to_run.remote remote_executable
      , fun () -> Rpc_parallel.Remote_executable.delete remote_executable >>| ok_exn )
  in
  let%bind conn =
    Worker.spawn
      ?env
      ~how
      ~shutdown_on:Connection_closed
      ~redirect_stdout:`Dev_null
      ~redirect_stderr:`Dev_null
      ~on_failure:Error.raise
      ~connection_state_init_arg:()
      ()
  in
  let%map () = cleanup_fn () in
  conn
;;

let print_worker_env conn ~key =
  let%bind.Deferred.Or_error result =
    Worker.Connection.run conn ~f:Worker.functions.getenv ~arg:key
  in
  printf "WORKER: %s=%s\n" key (Option.value result ~default:"<none>");
  Deferred.Or_error.ok_unit
;;

let basic_test =
  Command.async_or_error
    ~summary:"Using environment variables for great good"
    (let%map_open.Command host =
       flag "host" (optional string) ~doc:"HOST run worker on HOST"
     in
     fun () ->
       let open Deferred.Or_error.Let_syntax in
       let key = "TEST_ENV_KEY" in
       let data = "potentially \"problematic\" \\\"test\\\" string ()!" in
       let%bind conn = spawn ~env:[ key, data ] ~host () in
       let%bind () = print_worker_env conn ~key in
       let%bind () = print_worker_env conn ~key:"SHOULD_NOT_EXIST" in
       return ())
    ~behave_nicely_in_pipeline:false
;;

let special_var =
  Command.async_or_error
    ~summary:"Child inherits variables that influence process execution"
    (let%map_open.Command host =
       flag "host" (optional string) ~doc:"HOST run worker on HOST"
     in
     fun () ->
       let open Deferred.Or_error.Let_syntax in
       let envvar = "OCAMLRUNPARAM" in
       let envval = "foo=bar" in
       (Unix.putenv [@ocaml.alert "-unsafe_multidomain"]) ~key:envvar ~data:envval;
       let%bind conn = spawn ~host () in
       let%bind () = print_worker_env conn ~key:envvar in
       let%bind conn = spawn ~host ~env:[ envvar, "foo=user-supplied" ] () in
       let%bind () = print_worker_env conn ~key:envvar in
       return ())
    ~behave_nicely_in_pipeline:false
;;

(* Test that env values containing shell metacharacters are passed through to the worker
   literally. Currently [env_for_ssh] uses sexp quoting (double quotes), which does not
   prevent shell expansion of $ and `. This test documents the current broken behavior; it
   should be updated when the CR-soon in [remote_executable.ml] is addressed. *)
let shell_metachar_test =
  Command.async_or_error
    ~summary:"Test env values with shell metacharacters over SSH"
    (let%map_open.Command host =
       flag "host" (optional string) ~doc:"HOST run worker on HOST"
     in
     fun () ->
       let open Deferred.Or_error.Let_syntax in
       let vars =
         [ "TEST_DOLLAR", "$WILL_EXPAND"
         ; "TEST_BACKTICK", "`echo injected`"
         ; "TEST_DOLLAR_PAREN", "$(echo injected)"
         ]
       in
       let%bind conn = spawn ~env:vars ~host () in
       Deferred.Or_error.List.iter vars ~how:`Sequential ~f:(fun (key, _) ->
         print_worker_env conn ~key))
    ~behave_nicely_in_pipeline:false
;;

let () =
  Command.group
    ~summary:"Environment Variable Tests"
    [ "basic-test", basic_test
    ; "special-var", special_var
    ; "shell-metachar-test", shell_metachar_test
    ]
  |> Rpc_parallel_krb_public.start_app ~krb_mode:For_unit_test
;;
