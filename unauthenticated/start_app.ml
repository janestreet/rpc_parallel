open! Core
open! Async

let backend = (module Backend : Rpc_parallel.Backend)
let backend_and_settings = Rpc_parallel.Backend_and_settings.T ((module Backend), ())

let start_app
  ?rpc_max_message_size
  ?rpc_buffer_age_limit
  ?rpc_handshake_timeout
  ?rpc_heartbeat_config
  ?when_parsing_succeeds
  ?complete_subcommands
  ?add_validate_parsing_flag
  ?argv
  command
  =
  Rpc_parallel.start_app
    ?rpc_max_message_size
    ?rpc_buffer_age_limit
    ?rpc_handshake_timeout
    ?rpc_heartbeat_config
    ?when_parsing_succeeds
    ?complete_subcommands
    ?add_validate_parsing_flag
    ?argv
    backend_and_settings
    command
;;

module For_testing = struct
  let initialize = Rpc_parallel.For_testing.initialize backend_and_settings
end

module Expert = struct
  let start_master_server_exn
    ?rpc_max_message_size
    ?rpc_buffer_age_limit
    ?rpc_handshake_timeout
    ?rpc_heartbeat_config
    ?pass_name
    =
    Rpc_parallel.Expert.start_master_server_exn
      ?rpc_max_message_size
      ?rpc_buffer_age_limit
      ?rpc_handshake_timeout
      ?rpc_heartbeat_config
      ?pass_name
      backend_and_settings
  ;;

  let worker_command = Rpc_parallel.Expert.worker_command backend
  let start_worker_server_exn = Rpc_parallel.Expert.start_worker_server_exn backend
end
