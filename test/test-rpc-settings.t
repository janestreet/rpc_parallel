# we get a timeout because sleeping for 5 seconds exceeds the heartbeat timeout
  $ APP_RPC_SETTINGS_FOR_TEST="((heartbeat_config((timeout 3s)(send_every 1s))))" run_one_line test/timeouts.exe timeout -sleep-for 5
  ((rpc_error  (Connection_closed   (*"No heartbeats received*"* (glob)

# we don't get a timeout because we are overriding the rpc settings with the RPC_PARALLEL_RPC_SETTINGS env var
  $ APP_RPC_SETTINGS_FOR_TEST="((heartbeat_config((timeout 3s)(send_every 1s))))" RPC_PARALLEL_RPC_SETTINGS="((heartbeat_config((timeout 8s)(send_every 2s))))" run_one_line test/timeouts.exe timeout -sleep-for 5
  unresponsive worker returned

# In all the below tests, we expect the rpc settings to always align. The
# APP_RPC_SETTINGS_FOR_TEST environment variable is used to supply arguments to
# the top-level Rpc_parallel.start_app call.

  $ run test/timeouts.exe rpc-settings
  master : ()
  spawned worker (client side) : ()
  spawned worker (server side) : ()
  served worker (client side) : ()
  served worker (server side) : ()

  $ APP_RPC_SETTINGS_FOR_TEST="((handshake_timeout 1s))" run test/timeouts.exe rpc-settings
  master : ((handshake_timeout 1s))
  spawned worker (client side) : ((handshake_timeout 1s))
  spawned worker (server side) : ((handshake_timeout 1s))
  served worker (client side) : ((handshake_timeout 1s))
  served worker (server side) : ((handshake_timeout 1s))

  $ RPC_PARALLEL_RPC_SETTINGS="((handshake_timeout 2s))" run test/timeouts.exe rpc-settings
  master : ((handshake_timeout 2s))
  spawned worker (client side) : ((handshake_timeout 2s))
  spawned worker (server side) : ((handshake_timeout 2s))
  served worker (client side) : ((handshake_timeout 2s))
  served worker (server side) : ((handshake_timeout 2s))

  $ APP_RPC_SETTINGS_FOR_TEST="((handshake_timeout 1s))" RPC_PARALLEL_RPC_SETTINGS="((handshake_timeout 2s))" run test/timeouts.exe rpc-settings
  master : ((handshake_timeout 2s))
  spawned worker (client side) : ((handshake_timeout 2s))
  spawned worker (server side) : ((handshake_timeout 2s))
  served worker (client side) : ((handshake_timeout 2s))
  served worker (server side) : ((handshake_timeout 2s))

  $ APP_RPC_SETTINGS_FOR_TEST="((handshake_timeout 1s) (max_message_size 1500))" RPC_PARALLEL_RPC_SETTINGS="((handshake_timeout 2s))" run test/timeouts.exe rpc-settings
  master : ((max_message_size 1500) (handshake_timeout 2s))
  spawned worker (client side) : ((max_message_size 1500) (handshake_timeout 2s))
  spawned worker (server side) : ((max_message_size 1500) (handshake_timeout 2s))
  served worker (client side) : ((max_message_size 1500) (handshake_timeout 2s))
  served worker (server side) : ((max_message_size 1500) (handshake_timeout 2s))

  $ APP_RPC_SETTINGS_FOR_TEST="((handshake_timeout 1s))" RPC_PARALLEL_RPC_SETTINGS="((handshake_timeout 2s) (max_message_size 1500))" run test/timeouts.exe rpc-settings
  master : ((max_message_size 1500) (handshake_timeout 2s))
  spawned worker (client side) : ((max_message_size 1500) (handshake_timeout 2s))
  spawned worker (server side) : ((max_message_size 1500) (handshake_timeout 2s))
  served worker (client side) : ((max_message_size 1500) (handshake_timeout 2s))
  served worker (server side) : ((max_message_size 1500) (handshake_timeout 2s))
