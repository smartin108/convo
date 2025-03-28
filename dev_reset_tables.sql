USE [Convo]
GO
/****** Object:  StoredProcedure [dev].[reset_tables]    Script Date: 3/27/2025 8:26:42 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER   procedure [dev].[reset_tables] @kill_switch int = 0 as 
begin

if @kill_switch != 0
	begin

	begin transaction

	exec dev.drop_foreign_keys 1

	truncate table dev.repo_multimedia
	truncate table dev.message_multimedia
	truncate table dev.message_uuid
	truncate table dev.text_message

	truncate table land.multimedia
	truncate table land.text_message

	alter table dev.message_uuid add constraint fk_text_message_message_uuid 
		foreign key (message_id) references dev.text_message (id)

	alter table dev.message_multimedia add constraint fk_message_multimedia_text_message 
		foreign key (message_id) references dev.text_message (id)

	alter table dev.message_multimedia add constraint fk_message_multimedia_repo_multimedia 
		foreign key (multimedia_id) references dev.repo_multimedia (id)

	commit

	end
else
	begin
	print('Tables were not truncated.')
	print('The kill switch was not disabled.')
	print('Use "exec dev.reset_tables <i>" where <i> is not equal 0 to disable kill switch.')
	end

end
