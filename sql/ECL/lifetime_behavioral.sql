-- BEHAVIOURAL SICR
with com_mk as (
select 
  min(pt_date) as pt_date,
  case when product_type IN ('RCL','AKL','EAL') then 
    case when tenor_in_month is not null then tenor_in_month else round(months_between(loan_maturity_date,loan_disbursement_date),0) end
  else tenor end as tenor,
  loan_no,
  stage,
  ecl_bucket,
  day_past_due
from dm.rep_fin_reg_com_master_kredit_ss_d 
where 
ecl_bucket='{{sicr_rule}}'
-- stage='{{sicr_rule}}'
and import_source='Digibank'
group by loan_no, stage, ecl_bucket, day_past_due, case when product_type IN ('RCL','AKL','EAL') then 
    case when tenor_in_month is not null then tenor_in_month else round(months_between(loan_maturity_date,loan_disbursement_date),0) end
  else tenor end
)
, sicr_population as (
select a.pt_Date, a.ec_number,a.import_source,a.loan_no,a.product_type,b.tenor,
b.pt_date AS SICR_DATE
,(CASE WHEN ec_number = 'EC2' THEN write_off_date
             WHEN ec_number = 'EC3' THEN settle_date
             END) as close_date 
,datediff(CASE WHEN ec_number = 'EC2' THEN write_off_date
             WHEN ec_number = 'EC3' THEN settle_date
             END, b.pt_date) as lifetime_days
,months_between(CASE WHEN ec_number = 'EC2' THEN write_off_date
             WHEN ec_number = 'EC3' THEN settle_date
             END, b.pt_date) as lifetime_m
From ecl.rep_fin_ecl_prep_master_data_ss_m a 
inner join com_mk b on a.loan_no=b.loan_no
where a.pt_date='{{pt_date}}'
and a.ec_number in('EC2','EC3')
)
, population as (
  select 
  pt_date, product_type, tenor, lifetime_days, lifetime_m
  ,count(loan_no) as count_loan
  ,avg(lifetime_Days) as average_lifetime_days
  ,STDDEV(lifetime_Days) as stdev_lifetime
  ,avg(lifetime_m) as average_lifetime_m
  ,STDDEV(lifetime_m) as stdev_lifetime_m
  from sicr_population group by pt_date, product_type, tenor, lifetime_days, lifetime_m
)
, final_output as (
select pt_date,product_type,tenor
  ,count(loan_no) as count_loan
  ,avg(lifetime_Days) as average_lifetime_days
  ,STDDEV(lifetime_Days) as stdev_lifetime
  ,avg(lifetime_m) as average_lifetime_m
  ,STDDEV(lifetime_m) as stdev_lifetime_m
from sicr_population
group by pt_date,product_type,tenor)
  select * from final_output;