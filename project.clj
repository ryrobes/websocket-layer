(defproject com.ryrobes/websocket-layer "0.1.12-SNAPSHOT"

  :description
  "A layer of glue for jetty and core.async"

  :url
  "https://github.com/rutledgepaulv/websocket-layer"

  :license
  {:name "MIT" :url "http://opensource.org/licenses/MIT"}

  :deploy-repositories
  [["releases" :clojars]
   ["snapshots" :clojars]]

  ;;:plugins [[lein-eftest "0.6.0"]]

  :dependencies
  [;[org.clojure/clojure "1.10.1"]
   ;[org.clojure/core.async "1.0.567"]
   ;[org.clojure/clojure "1.11.4"]
   [org.clojure/clojure "1.12.0"]
   [org.clojure/core.async "1.6.681"]
   [io.github.tonsky/fast-edn "1.1.1"]
   ;[com.ryrobes/puget "1.3.5-SNAPSHOT"]
   ;[io.aleph/dirigiste "0.1.5"]
   [info.sunng/ring-jetty9-adapter "0.12.5"]
   [metosin/jsonista "0.2.5"]
   ;[metosin/jsonista "0.3.10"]
   [com.cognitect/transit-clj "1.0.324"]]

  :profiles
  {:test
   {:dependencies
    [[org.eclipse.jetty.websocket/websocket-client "9.4.20.v20190813"]
     [stylefruits/gniazdo "1.1.3"]]}})
