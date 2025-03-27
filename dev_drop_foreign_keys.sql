USE [Convo]
GO
/****** Object:  StoredProcedure [dev].[reset_tables]    Script Date: 3/27/2025 8:26:42 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create or ALTER   procedure [dev].drop_foreign_keys @kill_switch int = 0 as 
begin

if @kill_switch != 0
	begin

	begin transaction

	alter table dev.message_multimedia drop constraint if exists fk_message_multimedia_text_message;
	alter table dev.message_multimedia drop constraint if exists fk_message_multimedia_repo_multimedia;
	alter table dev.message_uuid       drop constraint if exists fk_text_message_message_uuid;

	commit

	end
else
	begin
	print('The kill switch was not disabled.')
	print('Use "exec dev.reset_tables <i>" where <i> is not equal 0 to disable kill switch.')
	end

end
