USE [Convo]
GO
/****** Object:  StoredProcedure [dev].[load_messages]    Script Date: 3/25/2025 1:05:10 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER     procedure [dev].[load_messages] as 

begin

BEGIN TRANSACTION

/*
 *	load messages
 */

;with KEYED as (
	select 
		id
		, ROW_NUMBER() over (partition by concat_ws('~', source_timestamp, author) order by record_created) as RN
	from land.text_message
)
, prim as (
	select 
		source_timestamp
		, converted_timestamp
		, sequence_number
		, author
		, body
		, ct
		, cl
		, mms_m_size
		, mms_tr_id
		, mms_id
		, record_created
		, hashbytes('SHA2_256'
			, concat_ws('~',
				source_timestamp
				, sequence_number
				, author
				) 
			) as content_hash
	from land.text_message L
	inner join KEYED 
		on L.id = KEYED.id
	where KEYED.RN = 1
)
merge into dev.text_message T
using prim S
on T.content_hash = S.content_hash
when not matched by target
then insert (
	source_timestamp
	, converted_timestamp
	, sequence_number
	, author
	, body
	, ct
	, cl
	, mms_m_size
	, mms_tr_id
	, mms_id
	, content_hash
	, record_created
	)
values (
	S.source_timestamp
	, S.converted_timestamp
	, S.sequence_number
	, S.author
	, S.body
	, S.ct
	, S.cl
	, S.mms_m_size
	, S.mms_tr_id
	, S.mms_id
	, S.content_hash
	, S.record_created
	)
;
--rollback
commit

end
;
