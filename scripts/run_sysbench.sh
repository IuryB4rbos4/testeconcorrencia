#!/usr/bin/env bash
set -e
threads=$1
time=${2:-180}
outdir=${3:-./results}
mkdir -p "$outdir"
sysbench oltp_read_write --tables=10 --table-size=100000 \
  --threads=$threads --time=$time --report-interval=10 \
  --db-driver=pgsql --pgsql-host=postgres --pgsql-user=pguser --pgsql-password=pgpass --pgsql-db=sbtest run \
  | tee "${outdir}/sysbench_t${threads}_t${time}.log"