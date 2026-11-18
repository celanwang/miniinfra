# Contributing

## Component branches

Develop each component on its own branch off `main` (`dev/minidb`, `dev/minikv`, or `dev/minimq`). Keep component changes in the matching directory and submit shared changes separately. Sync the branch with `main` before merging.

## Local checks

Each component is a separate Go module. Run the checks from its directory.

```sh
go test -race -count=1 ./... && go vet ./... && go build ./...
```

GitHub Actions runs these checks on Linux for all three modules on pushes and pull requests to `main`.

## Commits

Use one English line in the form `type(scope): description`. Name the feature or subsystem you changed in the scope, and start the description with an imperative verb.

```text
feat(wal): batch transaction flushes
fix(leases): reject stale acknowledgments
test(admission): compare cache policies under load
```

## Changelog

After a component change lands on `main`, whoever merged it updates [CHANGELOG.md](CHANGELOG.md) in a follow-up commit based on the latest `main`. Add one short English bullet at the top of **Unreleased**, starting with the component name. Summarize the merged change in one entry even if it spans several commits.
