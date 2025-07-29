


; with ranked as (
select 
	id
	, source_timestamp
	, author
	--, DENSE_RANK() over (order by source_timestamp) as kr
	--, DENSE_RANK() over (partition by author order by source_timestamp) as kvr
	, DENSE_RANK() over (order by source_timestamp) - DENSE_RANK() over (partition by author order by source_timestamp ) as group_rank
from dbo.conv
) , grouped as (
select 
	author
	, min(source_timestamp) as ts_start
	, max(source_timestamp) as ts_end
from ranked
group by 
	author
	, group_rank
), pre_report as (
select 
	conv.author
	, min(conv.converted_timestamp) as start_timestamp
	, max(conv.converted_timestamp) as end_timestamp
	, grouped.ts_start
	, grouped.ts_end
	, count(1) as message_count
	, sum(len(conv.body)) as message_length
from conv
inner join grouped
	on conv.author = grouped.author
	and conv.source_timestamp between ts_start and ts_end
group by 
	conv.author
	, grouped.ts_start
	, grouped.ts_end
)
select 
	author
	, start_timestamp
	, end_timestamp
	, message_count
	, message_length
	, (ts_end - ts_start)/1000 as duration
from pre_report
order by start_timestamp
;
