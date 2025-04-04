USE [Convo]
GO
/****** Object:  StoredProcedure [dev].[load_messages]    Script Date: 4/1/2025 8:00:37 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER     procedure [dev].[load_messages] as 

begin

BEGIN TRANSACTION


drop table if exists #new_messages;
drop table if exists #message_uuid_hash;
drop table if exists #multimedia_uuid_hash;
drop table if exists #new_multimedia;



-- helper table associates landed message content with uuid
create table #new_messages (
	id bigint						-- the id in the production table
	, message_hash binary(32)
);


-- helper table identifies message content that was newly inserted to production
create table #message_uuid_hash (
	id bigint
	, seq_id int
	, uuid binary(16)
	, message_hash binary(32)
);


-- helper table captures the stitched multimedia and uuids
create table #multimedia_uuid_hash (
	multimedia_hash binary(32)
	, uuid binary(16)
	, multimedia_content varchar(max)
	, record_created datetime
);


-- helper table captures multimedia content inserted into the repo
create table #new_multimedia (
	id bigint
	, multimedia_hash binary(32)
);


/*
 *	step: load new messages to production table
 */


insert into #message_uuid_hash (id, seq_id, uuid, message_hash)
select 
	id
	, cast(part_seq as int)
	, part_uuid
	, hashbytes('sha2_256'
		, concat_ws('~'
			, message_service
			, message_date
			, message_timestamp
			, message_author
			, mms_m_size
			, mms_tr_id
			, mms_id
			, part_seq
			, part_ct
			, part_cl
			, part_text
		)
	) as message_hash
from land.text_message
;


;with keyed_message as (
	select 
		t.*
		, u.message_hash
		, row_number() over (partition by u.message_hash order by t.record_created asc) as rn
	from land.text_message t
	inner join #message_uuid_hash u
		on t.id = u.id
	)
insert into dev.text_message (
	message_service
	, message_date
	, message_timestamp
	, message_author
	, mms_m_size
	, mms_tr_id
	, mms_id
	, part_seq
	, part_ct
	, part_cl
	, part_text
	, message_hash
	, record_created
	)
output 
	inserted.id
	, inserted.message_hash
into #new_messages (id, message_hash)
select 
	k.message_service
	, k.message_date
	, k.message_timestamp
	, k.message_author
	, k.mms_m_size
	, k.mms_tr_id
	, k.mms_id
	, k.part_seq
	, k.part_ct
	, k.part_cl
	, k.part_text
	, k.message_hash
	, k.record_created
from keyed_message as k
left join dev.text_message as p
	on k.message_hash = p.message_hash
where 
	p.id is null
	and k.rn = 1
;


/*
 *	step: load new multimedia to production
 */


;with stitched_multimedia_content as (
	select
		m.part_uuid
		, string_agg(cast(m.chunk_data as varchar(max)), '') within group (order by m.chunk_number asc) as MIME_content
		, min(m.record_created) as record_created
	from land.multimedia m
	-- limit to content associated with a newly inserted message
	inner join #message_uuid_hash u
		on m.part_uuid = u.uuid
	inner join #new_messages c
		on u.message_hash = c.message_hash
	group by 
		m.part_uuid
)
insert into #multimedia_uuid_hash (multimedia_hash, multimedia_content, uuid, record_created)
select 
	hashbytes('sha2_256', MIME_content)
	, MIME_content
	, part_uuid
	, record_created
from stitched_multimedia_content
;


insert into dev.repo_multimedia (content, multimedia_hash, record_created)
output 
	inserted.id
	, inserted.multimedia_hash
into #new_multimedia
select distinct
	m.multimedia_content
	, m.multimedia_hash
	, m.record_created
from #multimedia_uuid_hash m
;


/*
 *	step: record the new message and multmedia content ids to the join table
 */


insert into dev.message_multimedia (message_id, multimedia_id, seq_id, record_created)
select 
	#new_messages.id
	, #new_multimedia.id
	, #message_uuid_hash.seq_id
	, #multimedia_uuid_hash.record_created
from #new_multimedia
inner join #multimedia_uuid_hash
	on #new_multimedia.multimedia_hash = #multimedia_uuid_hash.multimedia_hash
inner join #message_uuid_hash
	on #multimedia_uuid_hash.uuid = #message_uuid_hash.uuid
inner join #new_messages 
	on #message_uuid_hash.message_hash= #new_messages.message_hash
;


--rollback
commit


drop table if exists #new_messages;
drop table if exists #message_uuid_hash;
drop table if exists #multimedia_uuid_hash;
drop table if exists #new_multimedia;


end
;

