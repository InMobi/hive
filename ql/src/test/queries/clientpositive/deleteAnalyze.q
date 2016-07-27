set hive.stats.autogather=true;

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/testdeci2;

create table testdeci2(
id int,
amount decimal(10,3),
sales_tax decimal(10,3),
item string)
stored as orc location '${system:test.tmp.dir}/testdeci2';

insert into table testdeci2 values(1,12.123,12345.123,'desk1'),(2,123.123,1234.123,'desk2');

describe formatted testdeci2;

dfs -rmr ${system:test.tmp.dir}/testdeci2/000000_0;

describe formatted testdeci2 amount;

analyze table testdeci2 compute statistics for columns;

set hive.stats.fetch.column.stats=true;

analyze table testdeci2 compute statistics for columns;

explain
select s.id,
coalesce(d.amount,0) as sales,
coalesce(d.sales_tax,0) as tax
from testdeci2 s join testdeci2 d
on s.item=d.item and d.id=2;
