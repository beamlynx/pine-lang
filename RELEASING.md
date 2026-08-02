# Releasing pine-lang

pine-lang is versioned and released independently of `beamlynx-ui` and
`beamlynx-desktop` — do not conflate version numbers across the three.

`master` requires changes to land via pull request (branch protection)
rather than a direct push.

## Checklist

1. Create branch `release/X.Y.Z` from the current working branch.
2. Run the formatter: `clj -M:fmt fix`.
3. Bump `src/pine/version.clj` → `"X.Y.Z"`.
4. Bump the image tag in `playground.docker-compose.yml` → `ahmadnazir/pine:X.Y.Z` — this is the source of truth for the deployed playground server version (not `beamlynx-cli/docker-compose.yml`, which is stale).
5. Move the `## [Unreleased]` section in `CHANGELOG.md` into a new `## [X.Y.Z] - YYYY-MM-DD` section (today's date), leaving `## [Unreleased]` empty.
6. Run `./scripts/check-version-sync.sh` to confirm `src/pine/version.clj` and `playground.docker-compose.yml` agree.
7. Commit all changed files: `Release X.Y.Z: <short description of the unreleased changes>`.
8. Push the branch and open a PR against `master`.

## After merging

If `beamlynx-ui`'s minimum required server version (`RequiredVersion` in `constants.ts`) needs to move up to this release, that's a `beamlynx-ui` release of its own — see `beamlynx-ui/RELEASING.md`.

If `beamlynx-desktop` bundles a pinned commit/branch of pine-lang for desktop packaging (classpath-based grammar loading, jpackage build scripts, etc. — see `desktop/`), and this release includes fixes that pin depended on, update `beamlynx-desktop/bundled-versions.json`'s `pineLangRef` to this new tag (see `beamlynx-desktop/RELEASING.md`) rather than leaving it pointed at a raw commit SHA once a real tagged release is available.
