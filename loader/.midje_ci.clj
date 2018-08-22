;; Copyright (c) Trainline Limited, 2017. All rights reserved.
;; See LICENSE.txt in the project root for license information
(when-not (running-in-repl?)
  (change-defaults :emitter 'midje.emission.plugins.junit
                   :print-level :print-facts
                   :colorize false))
