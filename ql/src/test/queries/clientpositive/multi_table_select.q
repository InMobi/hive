create table t1 (key int, value string) partitioned by (p1 string);
create table t2 (key int, value string) partitioned by (p1 string, p2 string);

LOAD DATA LOCAL INPATH '../data/files/kv1.txt' OVERWRITE INTO TABLE t1 PARTITION (p1='x');
LOAD DATA LOCAL INPATH '../data/files/kv1.txt' OVERWRITE INTO TABLE t2 PARTITION (p1='y', p2='y1');

explain extended select t.value, sum(t.key) from t1 t where t.p1="x" group by t.value;

select t.value, sum(t.key) from t1 t where t.p1="x" group by t.value;

explain extended select t.value, sum(t.key) from t2 t where t.p1="x" group by t.value;

select t.value, sum(t.key) from t2 t where t.p1="x" group by t.value;

explain extended select t.value, sum(t.key) from t2 t where t.p1="y" group by t.value;

select t.value, sum(t.key) from t2 t where t.p1="y" group by t.value;

explain extended select t.value, sum(t.key) from t1,t2 t where t.p1="x" group by t.value;

select t.value, sum(t.key) from t1,t2 t where t.p1="x" group by t.value;

explain extended select t.value, sum(t.key) from t1,t2 t where (t.p1="x" OR t.p1="y") group by t.value;

select t.value, sum(t.key) from t1,t2 t where (t.p1="x" OR t.p1="y") group by t.value;