# lspmux fork

Public mirror of https://codeberg.org/ooojustin/lspmux carrying Justin's patches.
Use branch `justin` for the package; `upstream-main` records the upstream reference.

The orphan default branch `sync` owns only this README and the sync workflow,
keeping automation commits out of the fork history. Daily and manual runs fetch
https://codeberg.org/p2502/lspmux `main`. Codeberg outages skip without writes;
ancestors are a no-op. Clean rebases must pass package derivation evaluation
without import-from-derivation before an explicit lease-protected push to `justin`.
Failures preserve a unique `sync-attempt/<date>-<run>-<attempt>` branch and open or comment
on an issue for Justin. Conflict snapshots intentionally retain conflict markers.
Only `GITHUB_TOKEN` is used, with contents and issues write permissions.

Git cannot store both `sync` and `sync/<date>` refs; attempts use `sync-attempt/`.
