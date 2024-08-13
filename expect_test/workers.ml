open! Core
open Async

let () = Rpc_parallel_krb_public.For_testing.initialize [%here]

let%expect_test "" =
  let%bind sum = Sum_worker.main 10
  and product = Product_worker.main 10 in
  printf "sum: %d\n" sum;
  printf "product: %d\n" product;
  [%expect
    {|
    sum: 45
    product: 3628800
    |}];
  return ()
;;
