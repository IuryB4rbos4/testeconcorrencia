# run (exemplo: workload A, 50 threads)
OPT=/opt/ycsb
${OPT}/bin/ycsb run mongodb -s -P ${OPT}/workloads/workloada \
  -threads 50 -p operationcount=1000000 \
  -p "mongodb.url=mongodb://mongoadmin:mongopass@mongo:27017/ycsb?w=1" \
  | tee results/ycsb_mongo_run_wA_t50.log