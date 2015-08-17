create table t2( key string, value int);
explain select sum(u.value) value from t2 u group by u.key having sum(u.value) > 30;