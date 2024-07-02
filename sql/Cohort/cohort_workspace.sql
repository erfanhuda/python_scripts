with com_mk as (
    select * from dm.rep_fin_reg_com_master_kredit_ss_d where pt_date = last_day(pt_date) 
    and product_code in ('101', '102', '103', '104', '105', '106', '107', '108', '109', '110', '111', '112', '115', 'C01', 'SC1', 'SC2', 'M05')
    -- and loan_no = '1000024883955396640616646'
)
, com_cohort as (
    select 
    A.pt_date as base_date
    , B.pt_date as t_date
    , A.loan_no as base_loan
    , B.loan_no as t_loan
    , A.client_no as base_client
    , B.client_no as t_client
    , A.day_past_due_client as base_dpd_client
    , B.day_past_due_client as t_dpd_client
    , A.cur_balance as base_cur_balance
    , B.cur_balance as t_cur_balance
    , case when A.day_past_due_client = 0 then 1
           when A.day_past_due_client between 1 and 30 then 2
           when A.day_past_due_client between 31 and 60 then 3
           when A.day_past_due_client between 61 and 90 then 4
           when A.day_past_due_client between 91 and 120 then 5
           when A.day_past_due_client between 121 and 150 then 6
           when A.day_past_due_client between 151 and 180 then 7
           when A.day_past_due_client > 180 then 8 end as base_bucket_client
    , case when B.day_past_due_client > 90 then 1
           else 0 end as t_cohort_flag
    , A.product_code
    , case when A.product_code in ('101', '102') then 'SPL'
           when A.product_code in ('103', '104') then 'BCL'
           when A.product_code = '105' then 'SCL'
           when A.product_code = '106' then 'KPL'
           when A.product_code = '107' then 'EML'
           when A.product_code = '108' then 'RCL'
           when A.product_code = '109' then 'AKL'
           when A.product_code = '110' then 'EAL'
           when A.product_code = '111' then 'PYL'
           when A.product_code = '112' then 'UDL'
           when A.product_code in ('115', 'C01') then 'APL'
           when A.product_code in ('SC1', 'SC2') then 'SCF'
           when A.product_code = 'M05' then 'SME' end as product_type
    , A.tenor
    , nvl(A.tenor_in_month, A.tenor) as tenor_in_month
    , nvl(A.payment_freq,1) as payment_freq
    , A.int_real_rate
    , A.loan_disbursement_date
    from com_mk A, com_mk B
    where A.pt_date <= B.pt_date and A.loan_no = B.loan_no
)
, ever_cohort as (
    select *
    , months_between(t_date, base_date) + 1 as period
    , max(t_dpd_client) over (partition by base_date, base_loan order by t_date rows unbounded preceding) as ever_t_dpd_client
    , max(t_cohort_flag) over (partition by base_date, base_loan order by t_date rows unbounded preceding) as ever_t_cohort_flag
    from com_cohort
)
select base_date, t_date, period, tenor_in_month, product_type
, base_bucket_client 
, count(base_loan) as count_noa
, sum(base_cur_balance) total_os
, sum(case when ever_t_cohort_flag = 1 then 1 else 0 end) as noa_cohort  
, sum(case when ever_t_cohort_flag = 1 then base_cur_balance else 0 end) as os_cohort 
from ever_cohort
group by base_date, t_date, period, tenor_in_month, product_type, base_bucket_client
order by base_date, t_date, period, product_type, tenor_in_month, base_bucket_client