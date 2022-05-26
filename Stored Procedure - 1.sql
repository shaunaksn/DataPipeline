-- Stored procedure to create an aggregate table in snowflake

CREATE OR REPLACE PROCEDURE PROJECT_DB.LOADED_TABLES."FACT_CLAIM_AGG_SP"()
RETURNS NUMBER(38,0)
LANGUAGE SQL
EXECUTE AS OWNER
AS '
    begin
      create or replace table fact_claim_agg as (
      select dpr.prdt_name, dpr.prdt_lob, dpr.product_id, dpo.valid_fromdate, dpo.valid_todate, 
      dpo.POL_POLICYNUMBER as policy_id, (fpo.POLICYEXPIRATIONDATE_ID - fpo.POLICYEFFECTIVEDATE_ID) as days_since_policy_expired, fpo.INSUREDAGE,fpo.FIRSTINSURED_ID,fpo.SECONDINSURED_ID
      from PROJECT_DB.LOADED_TABLES.fact_policy fpo 
      join PROJECT_DB.LOADED_TABLES.dim_policy dpo on fpo.policy_id = dpo.policy_id and fpo.source_system = dpo.source_system
      join PROJECT_DB.LOADED_TABLES.dim_product dpr on fpo.product_id = dpr.product_id
      );
    end;
  ';

call PROJECT_DB.LOADED_TABLES.fact_claim_agg_sp();
