(ns pine.grammar
  "Builds a parser from a grammar file when the code is compiled, not when
  the server starts. Compiling a grammar's text is the slow part, about
  150 ms for pine.bnf, and AOT keeps the result."
  (:require [clojure.java.io :as io]
            [instaparse.core :as insta]))

(set! *warn-on-reflection* true)

(defmacro defparser
  "Like instaparse's defparser, for the grammar in the classpath resource
  `path`. defparser compiles the grammar ahead of time only when it is given
  the text itself, so this macro reads the file and passes its text."
  [name path & opts]
  `(insta/defparser ~name ~(slurp (io/resource path)) ~@opts))
