# Migrating off Rpc_parallel_unauthenticated

### Why you shouldn't use Rpc_parallel_unauthenticated

`Rpc_parallel_unauthenticated` doesn't do authentication, which means anyone can connect to RPC parallel workers and dispatch work to them.
Due to our plans to remove the intern firewall, "anyone" will soon include interns, which is the forcing function for making this push now.

`Rpc_parallel_krb` does the same things as `Rpc_parallel_unauthenticated`, but uses a krb-rpc connection which gives it the opportunity to
authenticate who is connecting and optionally encrypt the connection.

### Some caveats to keep in mind when migrating

- Your parallel workers must be running as the same user as your master or upgrading will break things.
  One signal that this might be the case is if you use `Rpc_parallel.Remote_executable.copy_to_host` and
  pass it a string containing something like `USER@HOST` instead of just a hostname.

- Your worker and master binaries must roll at the same time, because there's no backwards or forwards
  compatibility between these things. Typically these are run out of the same binary, but the `Expert`
  module allows them to be split up.
