/*WARNING: Tweak as required */
-- Helpful: https://www.justanalytics.com/blog/hive-tez-query-optimization

-- QUERY OPTIMISATION
SET mapred.max.split.size=256000000;
SET mapreduce.output.fileoutputformat.compress=false;
SET mapred.reduce.tasks=-1;
SET hive.exec.reducers.max=999;
SET hive.exec.compress.intermediate=true;
SET hive.cli.print.header=true;
SET hive.variable.substitute=true;

-- TEZ CONFIG
set hive.execution.engine=tez;
set hive.compute.splits.in.am=false;
set tez.am.resource.memory.mb=8192;
set tez.am.java.opts=-Xmx8192m;
set tez.reduce.memory.mb=8192;
set hive.tez.container.size=8192;
set hive.tez.java.opts=-Xmx8192m;

set mapreduce.task.timeout=1200000;
set mapreduce.map.memory.mb=5120;
set mapreduce.reduce.memory.mb=10240;
set mapreduce.map.java.opts=-Xmx4096m;
set mapreduce.reduce.java.opts=-Xmx8192m;


/* More Optimizations to test */
set hive.optimize.index.filter=true;
set hive.compute.query.using.stats=true; /*for select count(1)*/
/* Vectorization: improves scans, aggregations, filters and joins */
set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;
