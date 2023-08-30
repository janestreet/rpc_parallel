open! Core
open! Async

let backend = (module Backend : Rpc_parallel.Backend)

let backend_and_settings krb_mode =
  Rpc_parallel.Backend_and_settings.T ((module Backend), krb_mode)
;;

let start_app
  ?rpc_max_message_size
  ?rpc_buffer_age_limit
  ?rpc_handshake_timeout
  ?rpc_heartbeat_config
  ?when_parsing_succeeds
  ~krb_mode
  command
  =
  Rpc_parallel.start_app
    ?rpc_max_message_size
    ?rpc_buffer_age_limit
    ?rpc_handshake_timeout
    ?rpc_heartbeat_config
    ?when_parsing_succeeds
    (backend_and_settings krb_mode)
    command
;;

module For_testing = struct
  let initialize here =
    Rpc_parallel.For_testing.initialize (backend_and_settings Mode.for_unit_test) here
  ;;
end

module Expert = struct
  let start_master_server_exn
    ?rpc_max_message_size
    ?rpc_buffer_age_limit
    ?rpc_handshake_timeout
    ?rpc_heartbeat_config
    ?pass_name
    ~krb_mode
    ~worker_command_args
    ()
    =
    Rpc_parallel.Expert.start_master_server_exn
      ?rpc_max_message_size
      ?rpc_buffer_age_limit
      ?rpc_handshake_timeout
      ?rpc_heartbeat_config
      ?pass_name
      (backend_and_settings krb_mode)
      ~worker_command_args
      ()
  ;;

  let worker_command = Rpc_parallel.Expert.worker_command backend
end
