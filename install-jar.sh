#!/bin/bash

./gradlew -PscalaVersion=2.13 -PskipSigning :streams:publishToMavenLocal
./gradlew -PscalaVersion=2.13 -PskipSigning :clients:publishToMavenLocal

