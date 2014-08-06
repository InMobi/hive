drop table if exists staging;
drop table if exists parquet_jointable1;
drop table if exists parquet_jointable2;
drop table if exists parquet_jointable1_bucketed_sorted;
drop table if exists parquet_jointable2_bucketed_sorted;

create table staging (key int, value string) stored as textfile;
insert into table staging select * from src order by key limit 2;

create table parquet_jointable1 stored as parquet as select * from staging;

create table parquet_jointable2 stored as parquet as select key,key+1,concat(value,"value") as myvalue from staging;

explain select p2.myvalue from parquet_jointable1 p1 join parquet_jointable2 p2 on p1.key=p2.key;
select p2.myvalue from parquet_jointable1 p1 join parquet_jointable2 p2 on p1.key=p2.key;

set hive.auto.convert.join=true;

explain select p2.myvalue from parquet_jointable1 p1 join parquet_jointable2 p2 on p1.key=p2.key;
select p2.myvalue from parquet_jointable1 p1 join parquet_jointable2 p2 on p1.key=p2.key;

set hive.optimize.bucketmapjoin=true;
set hive.optimize.bucketmapjoin.sortedmerge=true;
set hive.auto.convert.sortmerge.join=true;
set hive.input.format=org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;

create table parquet_jointable1_bucketed_sorted (key int,value string) clustered by (key) sorted by (key ASC) INTO 1 BUCKETS stored as parquet;
insert overwrite table parquet_jointable1_bucketed_sorted select key,concat(value,"value1") as value from staging cluster by key;
create table parquet_jointable2_bucketed_sorted (key int,value1 string, value2 string) clustered by (key) sorted by (key ASC) INTO 1 BUCKETS stored as parquet;
insert overwrite table parquet_jointable2_bucketed_sorted select key,concat(value,"value2-1") as value1,concat(value,"value2-2") as value2 from staging cluster by key;
explain select p1.value,p2.value2 from parquet_jointable1_bucketed_sorted p1 join parquet_jointable2_bucketed_sorted p2 on p1.key=p2.key;
select p1.value,p2.value2 from parquet_jointable1_bucketed_sorted p1 join parquet_jointable2_bucketed_sorted p2 on p1.key=p2.key;
