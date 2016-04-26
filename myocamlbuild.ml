(* OASIS_START *)
(* OASIS_STOP *)

module JS = Jane_street_ocamlbuild_goodies

let dev_mode = true

let dispatch = function
  | After_rules ->
    let hack = "ugly_hack_to_workaround_ocamlbuild_nightmare" in
    mark_tag_used hack;
    dep [hack] [hack];

    let lib_core_mods =
          [ "std"
          ; "parallel"
          ; "worker_id"
          ]
    in

    let add_exts l exts =
      List.concat (List.map (fun fn ->
        let fn = "core/src/" ^ fn in
        List.map (fun ext -> fn ^ ext)  exts)
        l)
    in

    rule hack
      ~prod:hack
      ~deps:(add_exts lib_core_mods [".cmx"; ".cmi"; ".cmo"])
      (fun _ _ ->
         let to_remove =
           add_exts lib_core_mods [ ".cmx"
                                  ; ".cmi"
                                  ; ".cmo"
                                  ; ".ml"
                                  ; ".mli"
                                  ; ".ml.depends"
                                  ; ".mli.depends"
                                  ; ".o"
                                  ]
         in
         Seq
           [ Seq (List.map rm_f to_remove)
           ; Echo ([], hack) ])
  | _ ->
    ()

let () =
  Ocamlbuild_plugin.dispatch (fun hook ->
    JS.alt_cmxs_of_cmxa_rule hook;
    JS.pass_predicates_to_ocamldep hook;
    if dev_mode && not Sys.win32 then JS.track_external_deps hook;
    Ppx_driver_ocamlbuild.dispatch hook;
    dispatch hook;
    dispatch_default hook)
