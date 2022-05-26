-- Stored Procedure to create an audit table

CREATE OR REPLACE PROCEDURE PROJECT_DB.LOADED_TABLES."AUDIT_TABLE_SP"()
RETURNS number(19,0)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
    begin
    
       -- desc procedure table for name and sql
        create or replace table PROJECT_DB.LOADED_TABLES.desc_procedure as (select * from PROJECT_DB.INFORMATION_SCHEMA.PROCEDURES);
        
        -- query history columns
        create or replace table PROJECT_DB.LOADED_TABLES.query_history as (select query_id, execution_status, start_time, end_time, rows_produced, user_name, 
        schema_name from table(PROJECT_DB.information_schema.query_history_by_session())
        WHERE query_text LIKE ''%sp%'' and query_text NOT LIKE ''%select%'');
        
        -- show procedures description column
        show procedures;
        create or replace table PROJECT_DB.LOADED_TABLES.show_procedure_description as (select * from table(result_scan(last_query_id())));
        
        -- creation of audit table
        create or replace table PROJECT_DB.LOADED_TABLES.audit_table as (
        select qh.query_id as batch_id, dp.procedure_name as sp_name, qh.execution_status as status, qh.start_time, qh.end_time,
        spd."description", dp.procedure_definition as SQL, qh.rows_produced as rows_affected, qh.user_name as executing_user
        from PROJECT_DB.LOADED_TABLES.query_history qh join PROJECT_DB.LOADED_TABLES.desc_procedure dp
        on qh.schema_name = dp.procedure_schema
        join PROJECT_DB.LOADED_TABLES.show_procedure_description spd 
        on spd."schema_name" = qh.schema_name
        );
        
    end;
  ';
  
call PROJECT_DB.LOADED_TABLES.audit_table_sp();
