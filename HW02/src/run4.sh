#!/bin/bash

sql="src/lib/mysql-connector-java.jar"
csv="src/lib/opencsv-3.9.jar"

java -jar SQLProc.jar $1 -csv $2