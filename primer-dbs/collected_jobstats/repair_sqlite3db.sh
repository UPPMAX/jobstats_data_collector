#!/bin/bash

DB_NAME=$1

echo '.dump'| sqlite3 $DB_NAME | sqlite3 $DB_NAME.repaired
mv $DB_NAME $DB_NAME.corrupt
mv $DB_NAME.repaired $DB_NAME

