open! Core

(** In order for expect tests to work properly, we need the following to happen before any
    tests are run, but after all the definitions of any workers used in the tests:

    - Master initialization In the main process, we must start the master server.

    - Worker initialization In spawned processes, we must start the worker server and
      start the Async scheduler.

    We need to be especially careful in the following case: a.ml: defines worker A b.ml:
    defines worker B do.ml: uses worker A and worker B

    Assuming a.ml and b.ml each have their own expect tests, they will need worker
    initialization code at their top levels. However, do.ml needs the worker
    initialization code to run after the definitions of the workers in a.ml and b.ml.

    We use an environment variable to track the root of the dependency tree. We ensure
    that worker initialization only occurs at the root. *)

val set_initialize_source_code_position : Source_code_position.t -> unit
val worker_environment : unit -> (string * string) list
val worker_should_initialize : Source_code_position.t -> bool
