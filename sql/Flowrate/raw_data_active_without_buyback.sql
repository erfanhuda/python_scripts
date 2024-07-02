------ UPDATE MK MASTER BUYBACK -----
set spark.sql.hive.convertMetastoreOrc=true;
with base_buyback as (
  select settle_type
  , loan_no
  , prod_type
  , concat(substring(settle_date, 1,4),"-",substring(settle_date, 5,2),"-",substring(settle_date, 7,2)) as settle_date
  , principal
  , pt_date
  from dm.rep_db_mid_loan_receipt_tab_ss_d
  where settle_type = 'BUYBACK' and pt_date = concat(substring(settle_date, 1,4),"-",substring(settle_date, 5,2),"-",substring(settle_date, 7,2)) 
  and pt_date between '{{start_date}}' and '{{end_date}}'
  )
insert overwrite table id_finpro.master_base_buyback partition (pt_date) 
select a.loan_no, c.client_no, c.group_client_no, c.product_code, 
    case 
      when c.product_code='101' then 'SPL' 
      when c.product_code='102' then 'SPL' 
      when c.product_code='103' then 'BCL' 
      when c.product_code='104' then 'BCL' 
      when c.product_code='105' then 'SCL' 
      when c.product_code='106' then 'KPL' 
      when c.product_code='107' then 'EML' 
      when c.product_code='108' then 'RCL' 
      when c.product_code='109' then 'AKL' 
      when c.product_code='110' then 'EAL' 
      when c.product_code='111' then 'PYL' 
      when c.product_code='112' then 'UDL' 
      when c.product_code='115' then 'APL' 
      when c.product_code='C01' then 'APL' 
      when c.product_code='SC1' then 'SCF' 
      when c.product_code='SC2' then 'SCF' 
      when c.product_code='M05' then 'SME'
  else c.product_type end as product_type
  ,c.int_real_rate
  ,c.tenor
  ,nvl(c.tenor_in_month, case when c.product_code in ('108','109','110') then round(months_between(c.loan_maturity_date,c.loan_disbursement_date),0) else c.tenor end) as tenor_in_month
  ,nvl(c.payment_freq,1) as payment_freq
  ,c.day_past_due + 1 as day_past_due
  ,a.principal
  ,c.loan_disbursement_date
  ,c.loan_maturity_date
  ,a.settle_date
  ,c.loan_disb
  ,'BUYBACK' as import_source
  ,c.pt_date
  from base_buyback a
  left join dm.rep_fin_reg_com_master_kredit_ss_d c 
  on a.loan_no = c.loan_no 
  and c.pt_date = date_add(a.settle_date, -1)
  where c.pt_date between date_add('{{start_date}}',-1) and '{{end_date}}';

