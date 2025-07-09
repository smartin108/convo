use convo 

/* ========================

Transaction is not committed

delete duplicate rows lacking metadata

======================== */


begin transaction

;with sig as (
select 
	message_date
	, message_author
	, part_text
from Convo.dev.text_message t
group by 
	message_date
	, message_author
	, part_text
having count(1) > 1
)
--select *
delete t
from dev.text_message t
inner join sig
	on t.message_date = sig.message_date
	and t.message_author = sig.message_author
	and t.part_text = sig.part_text
where t.message_hash is null

select *
from Convo.dev.text_message t
where ISNULL(t.part_seq,0) <> -1
order by t.message_date desc

select 
	cast(t.message_timestamp as date) as message_dd
	, sum(case when t.message_author = 'andy' then 1 else 0 end) as ACount
	, sum(case when t.message_author = 'rebecca' then 1 else 0 end) as rCount
from Convo.dev.text_message t
where ISNULL(t.part_seq,0) <> -1
group by 
	cast(t.message_timestamp as date)
order by 1 desc

rollback