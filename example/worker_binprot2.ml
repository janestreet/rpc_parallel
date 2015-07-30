open Core.Std
open Async.Std
open Rpc_parallel.Std

(* Arguments that will be passed to a [Worker.t] *)
module Args = struct
  type t =
    { checkpoint_time : float
    ; max_elt         : int
    ; process_delay   : float
    ; num_elts        : int
    } with bin_io

  let default =
    { checkpoint_time = 1.
    ; max_elt         = 100
    ; process_delay   = 0.1
    ; num_elts        = 100
    }
end

module Worker = struct
  module T = struct
    (* A [Worker.t] implements two functions. [start_work worker] will return an
       [int Pipe.Reader.t] that contains updates of the current [sum] of the specific
       worker. The passed in worker is used so the two [Worker.t]'s can communicate. Each
       worker can only process some subset of elements. When a worker sees an element it
       cannot process, it will send it to the other worker to process *)
    type 'worker functions =
      { process_elts : ('worker, unit,    int Pipe.Reader.t) Parallel.Function.t
      ; start_work   : ('worker, 'worker, int Pipe.Reader.t) Parallel.Function.t
      }

    (* Each [Worker.t] has some internal state. Most of this is updated upon
       initialization *)
    type state =
      { mutable sum             : int
      ; mutable can_process     : int -> bool
      ; done_ivar               : unit Ivar.t
      ; mutable checkpoint_time : float
      ; mutable max_elt         : int
      ; mutable process_delay   : float
      ; mutable num_elts        : int
      }

    type init_arg = [`Even | `Odd] * Args.t with bin_io

    let init (worker_type, (args : Args.t)) =
      let state =
        { sum             = 0
        ; can_process     = (fun _ -> false)
        ; done_ivar       = Ivar.create ()
        ; checkpoint_time = 0.
        ; max_elt         = 0
        ; process_delay   = 0.
        ; num_elts        = 0
        }
      in
      state.checkpoint_time <- args.checkpoint_time;
      state.max_elt         <- args.max_elt;
      state.process_delay   <- args.process_delay;
      state.num_elts        <- args.num_elts;
      Random.self_init ();
      (match worker_type with
       | `Even -> state.can_process <- (fun i -> i % 2 = 0)
       | `Odd  -> state.can_process <- (fun i -> i % 2 = 1));
      return state

    (* Process the element by just adding it to a running sum *)
    let process state elt =
      assert (state.can_process elt);
      state.sum <- state.sum + elt

    module Functions(C:Parallel.Creator with type state := state) = struct
      let process_elts_impl state () =
        let reader, writer = Pipe.create () in
        (* Generate random elements. If [state.can_process elt] then process the element,
           otherwise send the element to the other worker to be processed *)
        don't_wait_for
          (Deferred.repeat_until_finished state.num_elts (fun count ->
             Clock.after (sec state.process_delay) >>= fun () ->
             if count = 0 then
               begin
                 Ivar.fill state.done_ivar ();
                 return (`Finished ())
               end
             else
               let elt = Random.int state.max_elt in
               if state.can_process elt then
                 begin
                   process state elt;
                   return (`Repeat (count - 1))
                 end
               else
                 Pipe.write writer elt
                 >>| fun () -> `Repeat (count - 1))
           >>| fun () ->
           Pipe.close writer);
        return reader

      let process_elts =
        C.create_pipe ~f:process_elts_impl
          ~bin_input:Unit.bin_t ~bin_output:Int.bin_t ()

      (* Send periodic checkpoints to the caller. Also, set up the communication with
         the passed in [Worker.t] *)
      let start_work_impl state worker =
        let reader, writer = Pipe.create () in
        don't_wait_for
          (C.run_exn worker ~f:process_elts ~arg:()
           >>= fun reader ->
           Pipe.iter reader ~f:(fun i -> return (process state i)));
        let stop = Ivar.read state.done_ivar in
        Clock.every' ~stop (sec state.checkpoint_time) (fun () ->
          Pipe.write writer state.sum);
        (stop >>> fun () -> Pipe.close writer);
        return reader

      let start_work =
        C.create_pipe ~f:start_work_impl ~bin_input:C.bin_worker ~bin_output:Int.bin_t ()

      let functions = { process_elts; start_work }
    end

  end
  include Parallel.Make_worker(T)
end

let handle_error worker err =
  failwiths (sprintf "error in %s" worker) err Error.sexp_of_t

let command =
  Command.async ~summary:"foo"
    Command.Spec.(
      empty
      +> flag "-checkpoint-time" (optional_with_default Args.default.checkpoint_time float)
           ~doc:" how often the workers send checkpoints"
      +> flag "-max-elt" (optional_with_default Args.default.max_elt int)
           ~doc:" maximum element a worker can randomly generate"
      +> flag "-process-delay" (optional_with_default Args.default.process_delay float)
           ~doc:" worker delay between generating elements"
      +> flag "-num-elts" (optional_with_default Args.default.num_elts int)
           ~doc:" number of elements a worker will generate"
    )
    (fun checkpoint_time max_elt process_delay num_elts () ->
       let args : Args.t = {checkpoint_time; max_elt; process_delay; num_elts} in
       (* Spawn the workers *)
       Worker.spawn_exn (`Even, args) ~on_failure:(handle_error "even worker")
       >>= fun even_worker ->
       Worker.spawn_exn (`Odd, args) ~on_failure:(handle_error "odd worker")
       >>= fun odd_worker ->
       (* Tell workers to start doing work *)
       Worker.run_exn even_worker ~f:Worker.functions.start_work ~arg:odd_worker
       >>= fun reader_even ->
       Worker.run_exn odd_worker  ~f:Worker.functions.start_work ~arg:even_worker
       >>= fun reader_odd ->
       Deferred.all_unit [
         Pipe.iter reader_even
           ~f:(fun  i -> return (Core.Std.Printf.printf "even checkpoint: %d\n%!" i));
         Pipe.iter reader_odd
            ~f:(fun i -> return (Core.Std.Printf.printf "odd checkpoint: %d\n%!"  i))
       ])

let () = Parallel.start_app command
