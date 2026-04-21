/****** Script for SelectTopNRows command from SSMS  ******/

; with 
ix as (
	select ROW_NUMBER() over (order by x.id) as ix  
	from (select top 1000 id from convo.dev.text_message) as x
)
, message_text as (
	select id
		, lower(trim(part_text)) as text
	from Convo.dev.text_message
	where part_seq != -1
)
, trigrams as (
SELECT distinct [id]
	, substring(text, ix.ix, 3) as tk
FROM message_text as t
cross join ix
where ix.ix < len(t.text) - 3
)
select * from trigrams 

