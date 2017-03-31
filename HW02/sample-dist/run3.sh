#!/bin/bash

sql="dist/lib/mysql-connector-java.jar"
csv="dist/lib/opencsv-3.9.jar"

#java -cp $sql:$csv -jar ./dist/SQLProc.jar ./sample/p3clustercfg ./sample/p3sqlfile 
java -jar SQLProc.jar $1 $2