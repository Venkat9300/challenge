select result(concat(da,"#",concat_ws("\;",collect_list(type)),"#",concat_ws("\;",collect_list(ValuesSum))))
from
(SELECT calendar.datefield AS da, x.type, x.ValuesSum
FROM 
(select da,type,cast(sum(value) as string) as ValuesSum from cha
group by da,type
order by da,type desc) x RIGHT JOIN calendar ON (DATE(x.da) = calendar.datefield))y
group by da;