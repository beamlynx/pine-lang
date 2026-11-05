(ns pine.cache
  "Small bounded caches that keep the most recently used entries."
  (:import [java.util LinkedHashMap Map]))

(set! *warn-on-reflection* true)

(defn lru
  "A cache that holds at most n entries and drops the least recently used."
  [n]
  (proxy [LinkedHashMap] [16 0.75 true]
    (removeEldestEntry [_] (> (.size ^Map this) n))))

(defn lookup
  "The value stored under k, or nil."
  [^Map cache k]
  (locking cache (.get cache k)))

(defn store!
  "Stores v under k and returns v."
  [^Map cache k v]
  (locking cache (.put cache k v))
  v)
