open! Core
open! Async
module Local_or_remote = Backend.Settings

let backend = (module Backend : Rpc_parallel.Backend)

let backend_and_settings local_or_remote =
  Rpc_parallel.Backend_and_settings.T ((module Backend), local_or_remote)
;;

let start_app
  ?rpc_max_message_size
  ?rpc_buffer_age_limit
  ?rpc_handshake_timeout
  ?rpc_heartbeat_config
  ?when_parsing_succeeds
  ?complete_subcommands
  ?add_validate_parsing_flag
  ?argv
  ~local_or_remote
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
    (backend_and_settings local_or_remote)
    command
;;

module For_testing = struct
  let initialize =
    Rpc_parallel.For_testing.initialize
      (backend_and_settings
         Local_or_remote.only_allow_local_workers_will_fail_to_connect_if_remote)
  ;;
end

module Expert = struct
  let start_master_server_exn
    ?rpc_max_message_size
    ?rpc_buffer_age_limit
    ?rpc_handshake_timeout
    ?rpc_heartbeat_config
    ?pass_name
    ~local_or_remote
    =
    Rpc_parallel.Expert.start_master_server_exn
      ?rpc_max_message_size
      ?rpc_buffer_age_limit
      ?rpc_handshake_timeout
      ?rpc_heartbeat_config
      ?pass_name
      (backend_and_settings local_or_remote)
  ;;

  let worker_command = Rpc_parallel.Expert.worker_command backend
  let start_worker_server_exn = Rpc_parallel.Expert.start_worker_server_exn backend
end
