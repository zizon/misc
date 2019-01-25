#!/bin/bash 
current=`dirname "${BASH_SOURCE-$0}"`
old=`pwd`
cd $current

classpath="."
for jar in `ls $current | grep jar$`;do
    classpath="$classpath:$jar"
done

java -cp $classpath com.sf.misc.antman.simple.server.Main 0.0.0.0 10010

cd $old

