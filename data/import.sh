#!/bin/sh

if test -z "$2" ; then
    echo "$0: name_of_db sql_zip"
    exit 1
fi

DB="$1"
FILE="$2"

dropdb $DB
createdb $DB
psql -d $DB -c"create extension postgis"
zcat $FILE | psql -d $DB

exit 0
