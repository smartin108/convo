use Convo
drop table if exists #ns;
drop table if exists #cumul_ns;
drop table if exists #ns_spaces;
drop table if exists #t;
drop table if exists #pct_negative;
GO


declare @sig float = 0.25;
declare @days as int = 1 + (select datediff(day, (select min(converted_timestamp) from dbo.conv), (select max(converted_timestamp) from dbo.conv)));


; with [hours] as (select distinct dateserial - 1 as h from ReferenceData.Ref.Dates where dateserial between 0 and 24)
, [minutes] as (select distinct dateserial - 1 as m from ReferenceData.Ref.Dates where dateserial between 0 and 60)
select 
	TIMEFROMPARTS(h, m, 0, 0, 0) as t
into #t
from [hours], [minutes]
;



select 
	converted_timestamp as ns_start
	, lead(converted_timestamp, 1) over (order by converted_timestamp) as ns_end
	, (lead(source_timestamp, 1) over (order by source_timestamp) - source_timestamp) / 1000 as negative_space_interval
into #ns
from dbo.conv c
;


declare @ns_total int = (select sum(negative_space_interval) from #ns);


select
	ns_start
	, ns_end
	, sum(negative_space_interval) over (order by negative_space_interval desc) as cumul_ns
into #cumul_ns
from #ns
where negative_space_interval is not null
;


select 
	cast(
		dateadd (
			minute
			, datediff (
				minute
				, 0
				, ns_start
			)
			, 0
		) as time
	) as ns_start
	, cast(
		dateadd (
			minute
			, datediff (
				minute
				, 0
				, ns_end 
			)
			, 0
		) as time
	) as ns_end
into #ns_spaces
from #cumul_ns
where cumul_ns < @ns_total * @sig
;



-- 
select 
	t.t
	, count(1) as ticks
	, cast(count(1) as float) / @days as pct_ticks
into #pct_negative
from #t t, #ns_spaces n
where 
	(
		n.ns_start < n.ns_end
		and t between n.ns_start and n.ns_end
	)
	or (
		n.ns_start > n.ns_end
		and not t between n.ns_end and n.ns_start
	)
group by t.t
order by t
;


select * from #pct_negative order by t

--select 
--	c.ns_start
--	, c.ns_end
--from #cumul_ns c
