#!/bin/bash

current=`dirname "${BASH_SOURCE-$0}"`
old=`pwd`

cd $current

#mvn package -DskipTests=true

dist="$current/target/dist"

rm -rf $dist
mkdir -p $dist

for jar in `find $current/target/ | grep jar$`;do
    cp -f $jar $dist
done

cp $current/start.sh $dist/ 
cp $current/log4j2.properties $dist/

cd $current/target
tar -cvzf dist.tar.gz dist
cd $old

