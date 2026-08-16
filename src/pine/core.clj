(ns pine.core
  (:require [pine.api :refer [app]]
            [ring.adapter.jetty :refer [run-jetty]])
  (:gen-class))

#_{:clj-kondo/ignore [:unused-binding]}
(defn -main [& args]
  ;; Defaults to loopback-only. This server has no authentication of any
  ;; kind (see pine.api's wide-open CORS). Binding all interfaces by
  ;; default would leave it reachable from the whole LAN, not just this
  ;; machine -- with no :host set, Jetty bound 0.0.0.0 (checked with
  ;; `ss -ltnp` before this change). The dockerized playground needs the
  ;; opposite -- Docker's own port publish
  ;; (playground.docker-compose.yml's "127.0.0.1:33333:33333") already
  ;; restricts host-side exposure, but the process inside the container
  ;; must still bind its own 0.0.0.0 for that publish to reach it at all --
  ;; so that compose file sets PINE_HOST=0.0.0.0 explicitly.
  (run-jetty app {:port 33333 :host (or (System/getenv "PINE_HOST") "127.0.0.1") :join? false}))

