> **Pine has moved.** It now lives in [beamlynx/beamlynx-desktop](https://github.com/beamlynx/beamlynx-desktop), in the [`pine/`](https://github.com/beamlynx/beamlynx-desktop/tree/main/pine) folder, with its full history. Open issues and pull requests there.
>
> **Why:** since November 2026, Pine, the Beamlynx interface and the desktop app are one repo. They always shipped together, inside the desktop app. Now a change that touches all three is one pull request, and one release builds them from the same commit. Beamlynx, Pine included, is open source under the AGPL-3.0.
>
> This repo is archived and read-only. Its tags and releases stay here. Old commits keep their PolyForm Noncommercial or Apache-2.0 license.

# Pine

## Dependencies

- Clojure `1.11.3`

### Dev

- [cljfmt](https://github.com/weavejester/cljfmt)


## Repl

From emacs:

- Connect: `M-x cider-jack-in`
- Disconnect: `M-x  cider-quit`

No need to run the repl manually (i.e. open a terminal and run it).

## Run

```
clj -M:run # or run-dev
```

or

```
./server.sh
```

## Dev

Fix clojure format issues:


```
clj -M:fmt fix
```

## Tests

Run tests:

```
clojure -M:test
```

## License

[PolyForm Noncommercial License 1.0.0](LICENSE) - free for noncommercial use. For commercial licensing, contact contact@grephyte.com.
