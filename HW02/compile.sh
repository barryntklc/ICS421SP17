#!/bin/bash

sql="src/lib/mysql-connector-java.jar"
csv="src/lib/opencsv-3.9.jar"

mkdir ./com
mkdir ./dist
javac -d ./com -classpath $sql:$csv:. @META-INF/sources.txt
#java -cp "./src/lib/mysql-connector-java.jar" SQLProc.SQLProc
cp -r ./src/lib/* ./dist
cp ./src/*.sh ./dist

#java -cp lib\*.jar:. com/SQLProc/SQLProc

#http://stackoverflow.com/questions/9689793/cant-execute-jar-file-no-main-manifest-attribute
jar cvmf META-INF/MANIFEST.MF ./dist/SQLProc.jar -C ./com .
rm -rf ./com