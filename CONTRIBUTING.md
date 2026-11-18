# Contributing

## Component branches

Develop each component on its own branch off `main` (`dev/minidb`, `dev/minikv`, or `dev/minimq`). Keep component changes in the matching directory and submit shared changes separately. Sync the branch with `main` before merging.

## Commits

Use one English line in the form `type(scope): description`. Use the component or area as the scope and start the description with an imperative verb.

```text
feat(minimq): add delayed delivery
```

## Changelog

After a component change lands on `main`, whoever merged it updates [CHANGELOG.md](CHANGELOG.md) in a follow-up commit based on the latest `main`. Add one short English bullet at the top of **Unreleased**, starting with the component name. Summarize the merged change in one entry even if it spans several commits.
