with ecl_population as (
    select pt_date, ec_number, loan_no, client_no, 
    case when product_code in (101, 102) then 'SPL'
         when product_code in (103, 104) then 'BCL'
         when product_code = 105 then 'SCL' 
         when product_code = 106 then 'KPL' 
         when product_code = 107 then 'EML' 
         when product_code = 108 then 'RCL' 
         when product_code = 109 then 'AKL' 
         when product_code = 110 then 'EAL' 
         when product_code = 111 then 'PYL' 
         when product_code = 112 then 'UDL' 
         when product_code = 115 then 'APL' 
         when product_code = 'C01' then 'APL' 
         when product_code in ('SC1', 'SC2') then 'SCF' 
         when product_code ='M05' then 'SME' 
         end as product_type
    , tenor
    , cur_balance, 
    case when day_past_due_acct = 0 then 1
         when day_past_due_acct between 1 and 30 then 2
         when day_past_due_acct between 31 and 60 then 3
         when day_past_due_acct between 61 and 90 then 4
         when day_past_due_acct between 90 and 120 then 5
         when day_past_due_acct between 121 and 150 then 6
         when day_past_due_acct between 151 and 180 then 7
         when day_past_due_acct > 180 then 8 end as bucket_account,
    case when day_past_due_client = 0 then 1
         when day_past_due_client between 1 and 30 then 2
         when day_past_due_client between 31 and 60 then 3
         when day_past_due_client between 61 and 90 then 4
         when day_past_due_client between 90 and 120 then 5
         when day_past_due_client between 121 and 150 then 6
         when day_past_due_client between 151 and 180 then 7
         when day_past_due_client > 180 then 8 end as bucket_client
    from ecl.rep_fin_ecl_prep_base_data_ss_m
    where pt_date > '2023-06-30'
    and pt_date = last_day(pt_date) 
)
, trackert as (
    select 
        BASE.*,
        TRACKER.pt_date as track_pt_date,
        TRACKER.ec_number as track_ec_number,
        TRACKER.bucket_account as track_bucket_account,
        TRACKER.bucket_client as track_bucket_client,
        TRACKER.cur_balance as track_cur_balance,
        months_between(TRACKER.pt_date, BASE.pt_Date) as period
    from ecl_population BASE, ecl_population TRACKER
    WHERE TRACKER.pt_date >= BASE.pt_date
    AND BASE.loan_no = TRACKER.loan_no
    AND BASE.ec_number = 'EC1'
)
select 
pt_date,
product_type,
tenor,
bucket_account,
bucket_client,
sum(cur_balance) as cur_balance,
track_ec_number,
track_bucket_account,
track_bucket_client,
period,
count(distinct loan_no) count_loan,
count(distinct client_no) count_client,
sum(track_cur_balance) as track_cur_balance
from trackert
group by pt_date,
product_type,
tenor,
bucket_account,
bucket_client,
track_ec_number,
track_bucket_account,
track_bucket_client,
period;
