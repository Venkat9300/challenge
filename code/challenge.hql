#Creation of table cha
CREATE TABLE cha (da STRING, type STRING, value int)
row format delimited
fields terminated by '\;'
LINES TERMINATED BY '\n';

#To load the given data
LOAD DATA INPATH '/user/cloudera/cha.csv'
INTO TABLE cha;


#Query to produce required results
select result(concat(da,"#",concat_ws("\;",collect_list(type)),"#",concat_ws("\;",collect_list(ValuesSum))))
from
(SELECT calendar.datefield AS da, x.type, x.ValuesSum
FROM 
(select da,type,cast(sum(value) as string) as ValuesSum from cha
group by da,type
order by da,type desc) x RIGHT JOIN calendar ON (DATE(x.da) = calendar.datefield))y
group by da;