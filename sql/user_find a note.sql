USE [Convo]
GO

SELECT *
  FROM [dbo].[conv_vw_extended]
where 
	part_text like '%reddit%'
	--cast(message_timestamp as date) = '2026-01-14'
order by message_timestamp


declare @root_date   bigint = 1768412932000;
declare @minutes_before int = 60;
declare @minutes_after  int = 60;

select *
from dbo.conv_vw_extended
where 1=1
	and message_date between @root_date - @minutes_before * 60000
						and  @root_date + @minutes_after  * 60000
order by message_date