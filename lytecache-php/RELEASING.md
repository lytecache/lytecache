# Releasing

`lytecache/lytecache` is published on [Packagist](https://packagist.org), Composer's default package registry. Unlike npm or PyPI, there is no `publish` command -- Packagist reads directly from the GitHub repository and its tags.

## One-time setup

1. Push this repository to GitHub as `lytecache/lytecache` (or your own org/name -- update `composer.json`'s `name`, `homepage`, and `support` fields to match if it differs).
2. Go to [packagist.org/packages/submit](https://packagist.org/packages/submit) and submit the GitHub repository URL. This registers the package once; Packagist reads `composer.json` directly from the repo.
3. On the package's Packagist settings page, enable the GitHub Service Hook (or GitHub App integration) so new tags are picked up automatically. Without it, you'd need to click "Update" on Packagist manually after every release.

That's the entire one-time setup. No credentials or tokens are stored in this repository for this.

## Cutting a release

1. Make sure `main` is green: `composer validate`, `composer stan`, `composer pint:test`, `composer test` all pass (CI enforces this on every push, but verify locally before tagging).
2. Update [CHANGELOG.md](CHANGELOG.md): move the `[Unreleased]` section's contents under a new `## [x.y.z] - YYYY-MM-DD` heading.
3. Commit the changelog update.
4. Tag the release using [semantic versioning](https://semver.org/) with a `v` prefix:
   ```bash
   git tag v0.1.0
   git push origin main --tags
   ```
5. Packagist picks up the new tag automatically (via the webhook from one-time setup) within a minute or two. Verify at `https://packagist.org/packages/lytecache/lytecache` that the new version appears.

That's it -- `composer require lytecache/lytecache` (or `composer require lytecache/lytecache:^0.1`) now resolves to the tagged version for anyone.

## No committed `composer.lock`

This is a library, not an application, so `composer.lock` is gitignored rather than committed -- the standard Composer convention (a lock file pins exact versions for reproducible *application* deployments, but a library should let each consumer, and each CI matrix leg, resolve its own compatible dependency graph against the version ranges in `composer.json`). CI therefore runs `composer update`, not `composer install`, so the PHP 8.2/8.3/8.4 matrix legs each pick dependency versions that actually support that PHP version, rather than all three being forced through one lock file resolved on a single PHP version.

## Versioning notes

- **Public API** covers everything documented in [README.md](README.md) and the core/Laravel classes under `src/`. A change to a method signature, the on-disk schema, or a type code (see [SPEC.md](SPEC.md)) is a breaking change and requires a major version bump once past `1.0.0`.
- Before `1.0.0`, minor versions (`0.x.0`) may include breaking changes, per semver's `0.y.z` convention -- but changes to the storage format itself are avoided even pre-1.0, since cross-language file compatibility (with the Python, Java, Node.js, and Go implementations) is a standing project-wide invariant, not just an API stability concern.
- Session and queue drivers are documented `v0.2` candidates (see [CHANGELOG.md](CHANGELOG.md)), not yet implemented.
