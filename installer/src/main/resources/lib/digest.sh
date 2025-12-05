#!/bin/sh

cd "$(readlink -f "$0" | xargs dirname | xargs dirname)" || exit 1
exec java -cp war/profiler.war org.apache.catalina.realm.RealmBase "$@"
