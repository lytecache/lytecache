# Releasing

`lytecache-go` is published as a standalone repo at [github.com/lytecache/lytecache-go](https://github.com/lytecache/lytecache-go), separate from this monorepo, because Go modules resolve by import path against a real repository. It's a pure library -- no CLI, no binaries to build -- so releasing it is just making the tagged source available there; `go get`/`go install` compile straight from it themselves.

Looking for the `lytecache` command-line tool instead? See [lytecache-cli/RELEASING.md](../lytecache-cli/RELEASING.md) -- a separate repo/release process, deliberately decoupled from this one so pulling in `cobra`/`readline` as CLI-only dependencies never affects consumers of this library.

## One-time setup

1. Create `lytecache/lytecache-go` on GitHub (empty is fine -- the release workflow pushes to it).
2. Generate a fine-grained GitHub PAT, `GO_SPLIT_REPO_TOKEN`, scoped to exactly that repo (Contents: Read and write, Metadata: Read), matching this project's one-token-per-target-repo convention (see `SPLIT_REPO_TOKEN` for lytecache-php).
3. Add it as a repository secret on this monorepo (Settings -> Secrets and variables -> Actions). `.github/workflows/go-release.yml` reads it.

## Cutting a release

1. Make sure `main` is green: `go build ./...`, `go vet ./...`, and `go test -race ./...` (from `lytecache-go/`) all pass -- CI enforces this on every push, but verify locally before tagging.
2. Update [CHANGELOG.md](CHANGELOG.md): move the `[Unreleased]` section's contents under a new `## [x.y.z] - YYYY-MM-DD` heading.
3. Commit the changelog update.
4. Tag using the monorepo-subdirectory convention (`lytecache-go/vX.Y.Z`), distinct from every other component's tag prefix so tagging one release can never trigger another's workflow:
   ```bash
   git tag lytecache-go/v0.2.0
   git push origin main --tags
   ```
   If this tag already exists locally pointing at an older commit (e.g. from before the CLI/split-repo work), delete and recreate it first: `git tag -d lytecache-go/v0.2.0 && git push origin :refs/tags/lytecache-go/v0.2.0` before retagging.
5. `.github/workflows/go-release.yml` splits `lytecache-go/` out via `git subtree split` and pushes it, along with a **plain** `v0.2.0` tag (the `lytecache-go/` prefix is stripped -- it only exists to disambiguate tags within this monorepo), to `lytecache/lytecache-go`, runs `go test -race ./...` there as a final sanity check, and opens a GitHub release with install instructions.
6. Verify: `go get github.com/lytecache/lytecache-go@v0.2.0` resolves, and the version appears on [pkg.go.dev](https://pkg.go.dev/github.com/lytecache/lytecache-go).

### If lytecache-cli depends on this release

Check `lytecache-cli/go.mod`'s `require github.com/lytecache/lytecache-go` line -- if it names the version you just cut (or an earlier one it's now safe to un-pin from `replace`), that means lytecache-cli was waiting on this release before it could cut its own. See [lytecache-cli/RELEASING.md](../lytecache-cli/RELEASING.md) for that side.

## Versioning notes

- **Public API** covers everything documented in [README.md](README.md) and [SPEC.md](SPEC.md) (the on-disk schema and type codes). A change to either is a breaking change and requires a major version bump once past `1.0.0`.
- Before `1.0.0`, minor versions (`0.x.0`) may include breaking changes, per semver's `0.y.z` convention -- but changes to the storage format itself are avoided even pre-1.0, since cross-language file compatibility (with the Python, Java, Node.js, and PHP implementations) is a standing project-wide invariant, not just an API stability concern.
- Session and queue drivers are documented `v0.2`+ candidates (see [CHANGELOG.md](CHANGELOG.md)), not yet implemented.
