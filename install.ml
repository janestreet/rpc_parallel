#use "topfind";;
#require "js-build-tools.oasis2opam_install";;

open Oasis2opam_install;;

generate ~package:"rpc_parallel"
  [ oasis_lib "rpc_parallel_core_deprecated"
  ; file "META" ~section:"lib"
  ]
