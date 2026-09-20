# Releasing pine-lang

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
9. Once the PR merges, tag the merge commit on `master` and push the tag: `git tag X.Y.Z && git push origin X.Y.Z`. `beamlynx-desktop`'s `bundled-versions.json` pins a real tag rather than a SHA once one exists for the bundled version — skipping this leaves no tag for it to pin to.
10. Build and push the Docker image: `./build-image.sh`. It reads the version from `src/pine/version.clj`, checks whether `ahmadnazir/pine:X.Y.Z` is already in the registry, and builds and pushes a multi-platform image if it isn't. You need to be logged in to Docker Hub (`docker login`) as the account that owns `ahmadnazir/pine`.

    Nothing in CI does this — the image only exists because someone ran this script. Releases 0.39.0 through 0.45.0 were tagged without it, which left `playground.docker-compose.yml` naming image tags that were never published (the newest one in the registry was 0.38.2). Confirm the push landed before moving on:

    ```
    curl -s "https://hub.docker.com/v2/repositories/ahmadnazir/pine/tags?page_size=5&ordering=last_updated" | jq -r '.results[].name'
    ```

11. Deploy the playground. Bumping the image tag in `playground.docker-compose.yml` only changes what git says; it doesn't pull the image or restart anything. Log in to the playground host and run the deploy script, which pulls the new image and recreates the container:

    ```
    su -c 'cd ~/beamlynx/pine-lang/ && scripts/playground-deploy.sh' bot
    ```

    Do this as soon as the image is pushed. `beamlynx-ui`'s `RequiredVersion` (in its `constants.ts`) is often raised to this release in the same pass, and the playground web app deploys straight from `beamlynx-ui`'s `main` — so a UI that demands X.Y.Z can go live while the playground server is still on the old version, and every visitor gets the upgrade-required screen instead of the app.
