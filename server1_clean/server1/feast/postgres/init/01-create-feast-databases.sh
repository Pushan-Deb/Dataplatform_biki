#!/bin/sh
set -eu

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-SQL
  SELECT 'CREATE DATABASE feature_db'
  WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'feature_db')\gexec
SQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "feature_db" <<-SQL
  CREATE SCHEMA IF NOT EXISTS feast_online;
SQL
