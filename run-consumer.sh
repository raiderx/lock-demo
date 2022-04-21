#!/bin/bash

JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu17.30.15-ca-jdk17.0.1-macosx_x64
PATH=$JAVA_HOME/bin:$PATH

java -cp target/lock-demo-1.0-SNAPSHOT-jar-with-dependencies.jar \
        org.karpukhin.lockdemo.Consumer
