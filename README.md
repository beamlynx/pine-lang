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
