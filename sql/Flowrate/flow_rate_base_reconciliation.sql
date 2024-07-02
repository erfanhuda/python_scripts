

-- Query Recon OS
select pt_date, product_type,
case when day_past_due_client = 0 then '1.Current'
       when day_past_due_client between 1 and 30 then '2.1-30DPD'
       when day_past_due_client between 31 and 60 then '3.31-60DPD'
       when day_past_due_client between 61 and 90 then '4.61-90DPD'
       when day_past_due_client between 91 and 120 then '5.91-120DPD'
end as bucket_client,
sum(cur_balance) as cur_balance
from id_finpro.flow_rate_base_active
where pt_date between '{{start_date}}' and '{{end_date}}'
group by pt_date, product_type,
case when day_past_due_client = 0 then '1.Current'
       when day_past_due_client between 1 and 30 then '2.1-30DPD'
       when day_past_due_client between 31 and 60 then '3.31-60DPD'
       when day_past_due_client between 61 and 90 then '4.61-90DPD'
       when day_past_due_client between 91 and 120 then '5.91-120DPD'
end;

-- Query Recon OS All
select pt_date, product_type,
case when day_past_due_client = 0 then '1.Current'
       when day_past_due_client between 1 and 30 then '2.1-30DPD'
       when day_past_due_client between 31 and 60 then '3.31-60DPD'
       when day_past_due_client between 61 and 90 then '4.61-90DPD'
       when day_past_due_client between 91 and 120 then '5.91-120DPD'
end as bucket_client,
sum(cur_balance) as cur_balance
from id_finpro.flow_rate_base_all_new
where pt_date between '{{start_date}}' and '{{end_date}}'
group by pt_date, product_type,
case when day_past_due_client = 0 then '1.Current'
       when day_past_due_client between 1 and 30 then '2.1-30DPD'
       when day_past_due_client between 31 and 60 then '3.31-60DPD'
       when day_past_due_client between 61 and 90 then '4.61-90DPD'
       when day_past_due_client between 91 and 120 then '5.91-120DPD'
end;


-- Query Recon WO
select pt_date
  ,product_type
  ,sum(wo_principal) as wo_principal
  ,sum(wo_interest) as wo_interest
  ,sum(clawback_principal) as clawback_principal
  ,sum(clawback_interest) as clawback_interest
  ,sum(wo_principal_mtd) as wo_principal_mtd
  ,sum(wo_interest_mtd) as wo_interest_mtd
  ,sum(clawback_principal_mtd) as clawback_principal_mtd
  ,sum(clawback_interest_mtd) as clawback_interest_mtd
from id_finpro.flow_rate_base_wo
where pt_date between '{{start_date}}' and '{{end_date}}'
group by pt_date,product_type;

-- Query Recon WO
select pt_date
  ,product_type
  ,sum(wo_principal) as wo_principal
  ,sum(wo_interest) as wo_interest
  ,sum(clawback_principal) as clawback_principal
  ,sum(clawback_interest) as clawback_interest
  ,sum(wo_principal_mtd) as wo_principal_mtd
  ,sum(wo_interest_mtd) as wo_interest_mtd
  ,sum(clawback_principal_mtd) as clawback_principal_mtd
  ,sum(clawback_interest_mtd) as clawback_interest_mtd
from id_finpro.flow_rate_base_all_new
where pt_date between '{{start_date}}' and '{{end_date}}' and source = 'WRITE_OFF'
group by pt_date,product_type;


-- Query Recon Disbursement
select pt_date
, product_type
, tenor
, sum(principal )principal
, sum(principal_mtd) principal_mtd 
from id_finpro.flow_rate_base_disburse
where pt_date between '{{start_date}}' and '{{end_date}}'
group by pt_date, product_type, tenor;