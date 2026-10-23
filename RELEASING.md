# Releasing lytecache

Every package is released by pushing a git tag. Each tag prefix triggers
exactly one package's workflow in `.github/workflows/`, so tagging one
package never releases another.

| Package | Tag | Workflow | Publishes to |
|---|---|---|---|
| Python | `python-vX.Y.Z` | `release.yml` | [PyPI](https://pypi.org/project/lytecache/) |
| Node | `node-vX.Y.Z` | `node-release.yml` | [npm](https://www.npmjs.com/package/lytecache) |
| Java | `java-vX.Y.Z` | `java-release.yml` | [Maven Central](https://mvnrepository.com/artifact/io.github.lytecache/lytecache) |
| PHP | `vX.Y.Z` (bare, no prefix) | `split-php.yml` | [Packagist](https://packagist.org/packages/lytecache/lytecache), via the `lytecache/lytecache-php` repo |
| Go | `lytecache-go/vX.Y.Z` | `go-release.yml` | [`lytecache/lytecache-go`](https://github.com/lytecache/lytecache-go) as `vX.Y.Z` |
| CLI | `lytecache-cli/vX.Y.Z` | `lytecache-cli-release.yml`, `docker.yml` | [`lytecache/lytecache-cli`](https://github.com/lytecache/lytecache-cli) (binaries, Homebrew, Scoop), plus `ghcr.io/lytecache/lytecache` |

PHP uses a bare `v` tag because Packagist reads git tags as version numbers
and does not accept a prefix such as `php-v0.3.0`.

One-time setup (secrets, trusted publishers, split repos) is already done
and is documented in each package's own guide:
[Go](lytecache-go/RELEASING.md), [CLI](lytecache-cli/RELEASING.md),
[PHP](lytecache-php/RELEASING.md). The workflow files also explain their
own steps in comments.

## Published versions (as of 2026-10-22)

| Package | Latest published | Suggested next |
|---|---|---|
| Python | 0.2.0 | **0.3.0** |
| Node | 0.2.0 | **0.3.0** |
| Java | 0.2.0 | **0.3.0** |
| PHP | v0.2.0 | **v0.3.0** |
| Go | v0.3.1 | **v0.4.0** |
| CLI | v0.2.7 | **v0.3.0** |

Hashes and events are new features, so each package takes a minor bump.
For the CLI it is also a behaviour change, because `watch` now tails events
and the old stats view moved to `stats --watch`. Before 1.0, semver allows
that in a minor release.

## Release order

1. **Go first.** The CLI depends on the new lytecache-go APIs.
2. **CLI second.** Bump its `go.mod` to the Go version you just released.
3. **Python, Node, Java and PHP** in any order, after or alongside the two above.

The CLI release fails if the lytecache-go version named in
`lytecache-cli/go.mod` doesn't exist yet. The release workflow runs the
CLI's tests in the standalone repo against the real published module, not
against your local copy.

## Before you start

- **Commit everything** to `main` and push it. A tag releases the commit it
  points at, so anything uncommitted is not in the release.
- **Don't commit `go.work`.** It exists only so the CLI builds against the
  local `lytecache-go/` during development. If you commit it, monorepo CI
  for the CLI keeps passing against the local copy, and a missing
  `go.mod` bump goes unnoticed until the release job. Add it to
  `.gitignore`, or delete it once Go is released.
- **Make sure CI on `main` is green** before tagging.
- **Push one tag at a time** with `git push origin <tag>`, not `--tags`.
  That way you only trigger the release you mean to.
- **Published versions are permanent.** PyPI, npm and Maven Central never
  let you reuse a version number, even after deleting it. If a release goes
  wrong after it has published, fix the problem and release the next patch
  version (see [If something goes wrong](#if-something-goes-wrong)).

## Steps for every package

The same four steps apply each time:

1. **Bump the version** in the files listed for that package below.
2. **Update its `CHANGELOG.md`.** Rename `## [Unreleased]` to
   `## [X.Y.Z] - YYYY-MM-DD`, and add a fresh empty `## [Unreleased]` above
   it.
3. **Run the checks locally, then commit and push to `main`.**
4. **Tag the commit and push the tag.**

### 1. Go — `lytecache-go/vX.Y.Z`

No version string lives in the code. The tag is the version.

```bash
cd lytecache-go
go vet ./... && go test -race ./...
# edit CHANGELOG.md
cd ..
git add lytecache-go && git commit -m "lytecache-go: release v0.4.0"
git push origin main
git tag lytecache-go/v0.4.0
git push origin lytecache-go/v0.4.0
```

To verify, wait for the workflow to finish, then:

```bash
GOPROXY=https://proxy.golang.org go list -m github.com/lytecache/lytecache-go@v0.4.0
```

The new version should also appear on
[pkg.go.dev](https://pkg.go.dev/github.com/lytecache/lytecache-go). The
proxy can take a few minutes to pick up a new tag.

### 2. CLI — `lytecache-cli/vX.Y.Z`

The version comes from the tag (GoReleaser injects it with `-ldflags`).
What you do have to bump is the lytecache-go dependency:

```bash
cd lytecache-cli
GOWORK=off go get github.com/lytecache/lytecache-go@v0.4.0
GOWORK=off go mod tidy
GOWORK=off go vet ./... && GOWORK=off go test -race ./...   # proves it builds against the *published* module
```

Then edit `CHANGELOG.md`. It currently has **two** `## [Unreleased]`
sections. The second one is left over from the 0.2.x releases (the `ui`
work). Merge it into the new version's section, or date it as the 0.2.x
release it shipped in. Also delete the "Requires the unreleased
lytecache-go… bump `go.mod` before releasing" line, since you just did that.

```bash
cd ..
git add lytecache-cli && git commit -m "lytecache-cli: release v0.3.0"
git push origin main
git tag lytecache-cli/v0.3.0
git push origin lytecache-cli/v0.3.0
```

This one tag triggers two workflows. `lytecache-cli-release.yml` handles
the split repo, GoReleaser binaries, Homebrew and Scoop.
`docker.yml` builds the GHCR image.

To verify:

```bash
go install github.com/lytecache/lytecache-cli/cmd/lytecache@v0.3.0 && lytecache --version
brew upgrade lytecache/tap/lytecache        # or: brew install lytecache/tap/lytecache
docker pull ghcr.io/lytecache/lytecache:0.3.0
```

### 3. Python — `python-vX.Y.Z`

Bump the version in both places, and keep them identical:

- `lytecache-python/pyproject.toml`: `version = "0.3.0"`
- `lytecache-python/src/lytecache/__init__.py`: `__version__ = "0.3.0"`

In `CHANGELOG.md`, also give the old `## [0.2.0] - Unreleased` and
`## [0.1.0] - Unreleased` headings their real release dates.

```bash
cd lytecache-python
ruff check . && mypy src/ && pytest
python -m build && rm -rf dist    # optional: confirms the package builds
cd ..
git add lytecache-python && git commit -m "lytecache-python: release 0.3.0"
git push origin main
git tag python-v0.3.0
git push origin python-v0.3.0
```

The workflow uses PyPI trusted publishing (no token) and the GitHub
environment `release`. 0.2.0 has no `python-v` tag, so it was apparently
uploaded by hand. That makes this the first release through the workflow.
Before tagging, check two settings:

- On PyPI, under *Manage → Publishing*, a trusted publisher should exist
  for repo `lytecache/lytecache`, workflow `release.yml`, environment
  `release`.
- On GitHub, the `release` environment should exist. If it has required
  reviewers, the publish job waits for your approval in the Actions tab.

To verify: `pip install -U lytecache==0.3.0`.

### 4. Node — `node-vX.Y.Z`

```bash
cd lytecache-node
npm version 0.3.0 --no-git-tag-version   # updates package.json and package-lock.json
npm run typecheck && npm run lint && npm run build && npm test
cd ..
git add lytecache-node && git commit -m "lytecache-node: release 0.3.0"
git push origin main
git tag node-v0.3.0
git push origin node-v0.3.0
```

Run `npm version` with `--no-git-tag-version`. Otherwise npm creates its
own `v0.3.0` tag, and a bare `v` tag is the **PHP** release trigger.

In `CHANGELOG.md`, give `## [0.2.0] - Unreleased` its real date.

Publishing uses npm trusted publishing (OIDC). On npmjs.com, under the
package's *Settings → Trusted publishing*, there should be an entry for
`lytecache/lytecache` and workflow `node-release.yml`. If 0.2.0 was
published by hand, set that entry up before tagging, or the publish step
fails with an authentication error.

To verify: `npm view lytecache version`.

### 5. Java — `java-vX.Y.Z`

Bump the version in three places:

- `lytecache-java/build.gradle.kts`: `version = "0.3.0"`
- `lytecache-java/README.md`: the Gradle and Maven snippets (lines 24 and 33)
- `docs/index.html`: the Java install snippet (around lines 478 and 484)

```bash
cd lytecache-java
./gradlew clean build
cd ..
git add lytecache-java docs/index.html && git commit -m "lytecache-java: release 0.3.0"
git push origin main
git tag java-v0.3.0
git push origin java-v0.3.0
```

The workflow signs the artifacts, uploads them to Sonatype, and closes and
releases the staging repository. It uses the `SIGNING_KEY`,
`SIGNING_PASSWORD`, `SONATYPE_USERNAME` and `SONATYPE_PASSWORD` secrets.
Maven Central can take 10–30 minutes to show the new version after the
workflow succeeds, and mvnrepository.com takes longer.

To verify: check
`https://repo1.maven.org/maven2/io/github/lytecache/lytecache/0.3.0/`.

### 6. PHP — `vX.Y.Z`

`composer.json` has no version field, so the tag is the version.

```bash
cd lytecache-php
composer validate && composer stan && composer pint:test && composer test
cd ..
# edit CHANGELOG.md; also give "## [0.1.0] - Unreleased" its real date
git add lytecache-php && git commit -m "lytecache-php: release v0.3.0"
git push origin main
git tag v0.3.0
git push origin v0.3.0
```

`split-php.yml` pushes `lytecache-php/` and the tag to
`lytecache/lytecache-php`. Packagist then picks up the tag through its
webhook within a minute or two.

To verify: look for the version at
`https://packagist.org/packages/lytecache/lytecache`, or run
`composer require lytecache/lytecache:^0.3`.

## Releasing several packages from one commit

You can bump everything in one commit, push `main` once, and then push the
tags one by one. Keep the order: Go, then wait for it to go green, then the
CLI, then the rest.

```bash
git tag lytecache-go/v0.4.0 && git push origin lytecache-go/v0.4.0
# ...wait for "Release Go library" to pass, then bump lytecache-cli/go.mod,
# commit, and push. The CLI tag must point at that later commit...
git tag lytecache-cli/v0.3.0 && git push origin lytecache-cli/v0.3.0
git tag python-v0.3.0        && git push origin python-v0.3.0
git tag node-v0.3.0          && git push origin node-v0.3.0
git tag java-v0.3.0          && git push origin java-v0.3.0
git tag v0.3.0               && git push origin v0.3.0
```

Watch each run in the Actions tab, or with `gh run list --limit 10`.

## If something goes wrong

**The workflow failed before anything was published.** For example, a
test or build step failed, or a secret was missing. Fix the problem,
commit, then move the tag to the new commit:

```bash
git tag -d python-v0.3.0
git push origin :refs/tags/python-v0.3.0
git tag python-v0.3.0
git push origin python-v0.3.0
```

**The package was already published.** You can't reuse the version on
PyPI, npm or Maven Central. Fix the problem and release the next patch,
such as `python-v0.3.1`. If the bad version is harmful, yank or deprecate
it:

- PyPI: the project's *Manage* page has a *Yank* option.
- npm: `npm deprecate lytecache@0.3.0 "use 0.3.1"`.
- Maven Central: releases cannot be removed. Only release a new version.

**Go, CLI or PHP.** These release by pushing a tag to their standalone
repo, and the workflows force-push that tag, so retagging works. Go has
one exception: once `proxy.golang.org` has cached a version, the proxy and
`go.sum` checksums keep that original content for good. Release a new
patch rather than retagging a Go version that anyone may have fetched.
