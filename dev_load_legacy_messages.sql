USE [Convo]
GO
/****** Object:  StoredProcedure [dev].[load_legacy_messages]    Script Date: 4/3/2025 8:12:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER procedure [dev].[load_legacy_messages]
as begin


begin transaction



insert into dev.text_message (
	[message_date]
	,[message_timestamp]
	,[message_author]
	,[part_text]
	,[record_created]
)

select 
	c.source_timestamp
	, c.converted_timestamp
	, c.author
	, c.body
	, c.record_created
from dbo.conv_save c
left join dev.text_message n
	on c.source_timestamp = n.message_date
	and c.author = n.message_author
where n.id is null
;




select *
from dev.text_message
order by message_date


--rollback transaction
commit transaction


end;

