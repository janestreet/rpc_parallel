open! Core.Std

(** A type-safe parallel library built on top of Async_rpc.

    module Worker = Parallel.Make (T : Worker_spec)

    The [Worker] module can be used to spawn new workers, either locally or remotely,
    and run functions on these workers. [T] specifies which functions can be run on a
    [Worker.t] as well as the implementations for these functions. In addition, [T]
    specifies worker states and connection states. See README for more details *)

module Parallel = struct
  include Parallel
  module Remote_executable   = Remote_executable
  module Executable_location = Executable_location
end

module Parallel_managed = Parallel_managed
module Map_reduce       = Map_reduce
