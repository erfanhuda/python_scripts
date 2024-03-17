WITH loan_repay as (
    select pt_date, loan_no, prod_type, tenor_contractual, tenor_in_month, payment_freq, int_real_rate, loan_disbursement_date, loan_maturity_date, accounting_status
    , repay_interest 
    from dm.rep_fin_reg_loan_repay_detail_ss_d
    where pt_date between '${start_date}' and '${end_date}')
, receipt_tab as (
    select pt_date, loan_no, disburse_date, interest_rate 
    from dm.rep_db_mid_loan_receipt_tab_ss_d
    where pt_date between '${start_date}' and '${end_date}')
, com_mk as (
  select pt_date, loan_no from dm.rep_fin_reg_com_master_kredit_ss_d
  where pt_date between '${start_date}' and '${end_date}' 
)
, A AS (
  select substring(a.pt_date,1,7) as report_month
  ,case 
        when a.prod_type in ('101','102') then 'SPL'
        when a.prod_type in ('103','104') then 'BCL'
        when a.prod_type in ('105') then 'SCL'
        when a.prod_type in ('106') then 'KPL'
        when a.prod_type in ('107') then 'EML'
        when a.prod_type in ('108') then 'RCL'
        when a.prod_type in ('109') then 'AKL'
        when a.prod_type in ('110') then 'EAL'
        when a.prod_type in ('111') then 'PYL'
        when a.prod_type in ('112') then 'UDL'
        when a.prod_type in ('115') then 'APL'
  end as product_type
  ,a.tenor_contractual as tenor
  ,case when a.prod_type IN ('108','109','110') then 
    nvl(a.tenor_in_month, round(months_between(a.loan_maturity_date,A.loan_disbursement_date),0)) else a.tenor_contractual end 
  as tenor_in_month
  ,a.payment_freq
  , nvl(a.int_real_rate,b.interest_rate) as int_real_Rate
  --,b.ecl_bucket_max
  ,substring(b.disburse_date,1,7) as disb_month
  ,sum(a.repay_interest) as repay_interest
  from loan_repay a 
  left join receipt_tab b on last_day(a.pt_date)=b.pt_date and a.loan_no=b.loan_no
  left join com_mk c on a.pt_date=c.pt_date and a.loan_no=c.loan_no
  where a.prod_type in ('101','102','103','104','105','106','107','108','109','110','111','112','115')
  and a.accounting_status!='WRITE_OFF'
  and a.pt_date between '${start_date}' and '${end_date}'
  group by substring(a.pt_date,1,7)
  ,case 
        when a.prod_type in ('101','102') then 'SPL'
        when a.prod_type in ('103','104') then 'BCL'
        when a.prod_type in ('105') then 'SCL'
        when a.prod_type in ('106') then 'KPL'
        when a.prod_type in ('107') then 'EML'
        when a.prod_type in ('108') then 'RCL'
        when a.prod_type in ('109') then 'AKL'
        when a.prod_type in ('110') then 'EAL'
        when a.prod_type in ('111') then 'PYL'      
        when a.prod_type in ('112') then 'UDL'
        when a.prod_type in ('115') then 'APL'
  end
  ,a.tenor_contractual
  ,case when a.prod_type IN ('108','109','110') then 
    nvl(a.tenor_in_month, round(months_between(a.loan_maturity_date,A.loan_disbursement_date),0)) else a.tenor_contractual end
  ,a.payment_freq
  , nvl(a.int_real_rate,b.interest_rate) 
  --,b.ecl_bucket_max
  ,substring(b.disburse_date,1,7)
) 
select * from A