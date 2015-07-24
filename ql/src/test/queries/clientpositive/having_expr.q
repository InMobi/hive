EXPLAIN SELECT sum(s.value) AS c FROM src s GROUP BY key HAVING sum(s.value) > 3;
SELECT sum(s.value) AS c FROM src s GROUP BY key HAVING sum(s.value) > 3;