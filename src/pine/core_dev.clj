(ns pine.core-dev
  (:require [pine.api :refer [app server-config]]
            [ring.adapter.jetty :refer [run-jetty]]
            [ring.middleware.reload :refer [wrap-reload]]))

#_{:clj-kondo/ignore [:unused-binding]}
(defn -main [& args]
  ;; Loopback by default, like pine.core. Without :host Jetty bound every
  ;; interface, so the dev server was reachable from the whole LAN.
  (let [host (or (System/getenv "PINE_HOST") "127.0.0.1")]
    (reset! server-config {:token (not-empty (System/getenv "PINE_TOKEN")) :host host})
    (run-jetty (wrap-reload #'app) {:port 33333 :host host :join? false})))
