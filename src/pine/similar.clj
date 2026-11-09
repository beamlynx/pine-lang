(ns pine.similar
  "Names close to one that was mistyped, for errors that suggest what was
  meant."
  (:require
   [clojure.string :as s]))

(set! *warn-on-reflection* true)

(defn edit-distance
  "How many single-character insertions, deletions or substitutions turn
  `a` into `b`."
  [a b]
  (let [n (count b)]
    (peek
     (reduce (fn [prev [i ca]]
               (reduce (fn [row [j cb]]
                         (conj row (min (inc (peek row))
                                        (inc (nth prev (inc j)))
                                        (+ (nth prev j) (if (= ca cb) 0 1)))))
                       [(inc i)]
                       (map-indexed vector b)))
             (vec (range (inc n)))
             (map-indexed vector a)))))

(defn- fold
  "A name without case or underscores, so `tenant_id`, `tenantId` and
  `TENANT_ID` are the same."
  [name]
  (s/replace (s/lower-case name) "_" ""))

(defn close-names
  "The names in `candidates` closest to `name`, best first, at most three.
  Case and underscores are ignored, so `tenant_id` finds `tenantId`. A name
  that differs only in those is all that is returned. Otherwise a name may
  differ by up to two characters, or one for a short name."
  [name candidates]
  (let [folded  (fold name)
        same    (filter #(= folded (fold %)) candidates)
        allowed (if (<= (count folded) 4) 1 2)]
    (if (seq same)
      (vec (distinct same))
      (->> candidates
           distinct
           (map (fn [c] [c (edit-distance folded (fold c))]))
           (filter #(<= (second %) allowed))
           (sort-by second)
           (take 3)
           (mapv first)))))

(defn did-you-mean
  "\"Did you mean `a` or `b`?\", or nil when nothing is close."
  [name candidates]
  (when-let [names (seq (map #(str "`" % "`") (close-names name candidates)))]
    (str "Did you mean "
         (if (next names)
           (str (s/join ", " (butlast names)) " or " (last names))
           (first names))
         "?")))
