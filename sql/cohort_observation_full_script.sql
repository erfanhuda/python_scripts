with default_info as 
(select loan_no
,client_no
,last_day(min(accounting_date)) as first_default_date
from dwd.t05_pl_loan_accounting_status_change_evt_ss_d 
where pt_date='{{dt}}' 
and accounting_status_code ='NON_PERFORMING'
and accounting_date <='{{dt}}'
group by loan_no,client_no) 

,default_info_cif as 
    (select client_no
    ,min(default_info.first_default_date) as first_default_date_client
    from default_info
    group by client_no)

,prep_base_data as (

    select
    last_day(prep_base.loan_disbursement_date) 												as pt_date
    --,prep_base.pt_date																				as position_date
    ,nvl(months_between(pt_date,last_day(prep_base.loan_disbursement_date)),0)		as max_period
    ---revised to base on first_default_date
    ,case when default_info.first_default_date is null then 0
    else months_between(pt_date,default_info.first_default_date)	end			as max_period_default														
    ,prep_base.loan_no																		as loan_no
    ,prep_base.client_no																	as client_no
    ,prep_base.product_code																	as prod_code
    ,prep_base.product_type																	as prod_desc
    ,prep_base.tenor																		as tenor
    ,prep_base.collectability_acct															as collectability
    -- prep_base.first_default_date when loan become non-performing for outstanding (EC1) and wo (EC2) loans, default to empty for EC3 cannot meet pd_cohort difinition
    ,nvl(default_info.first_default_date,'')												as pd_cohort_first_default_date
    -- when loan become write-off for wo(EC2) loans ; default to empty for outstanding (EC1) and settled (E3) in prep_base
    ,nvl(case when default_info.first_default_date is not null then 'Y' end,'N')			as pd_cohort_default_flag 
    ,prep_base.write_off_date																as write_off_date  
    ,prep_base.int_real_rate																as int_real_rate
    ,concat(prep_base.IMPORT_SOURCE,"_", prep_base.ec_number)								as src_ec
    --- assumption in prep_base: ecl_bucket default to 5 and dpd default to 180 for wo (EC2); 
    -- ecl_bucket default to 1 and dpd default to 0 for settled (E3) loans 
    ,CASE WHEN PREP_BASE.EC_NUMBER = 'EC1' and PREP_BASE.ECL_BUCKET_ACCT <>'5' and PREP_BASE.DAY_PAST_DUE_ACCT<'90' and default_info.first_default_date is null THEN '0_Active'
    WHEN PREP_BASE.EC_NUMBER = 'EC3' THEN '2_Settled'
    WHEN PREP_BASE.EC_NUMBER = 'EC2' and default_info.first_default_date is not null THEN '3_WO'
    else '1_Default' end 																	as condition
    --pre_base: CASE WHEN MASTER_KREDIT.DAY_PAST_DUE > 90 THEN 'Y' ELSE 'N' END  and EC1; 'Y' and EC2; 'N' and EC3
    ,prep_base.default_flag_acct 															as wo_flag
    ,prep_base.ecl_bucket_acct																as ecl_bucket
    ,prep_base.cur_balance																	as cur_balance
    ,'acct level' 																			as import_source
    ,'EC1'																				 	as ec_number													
    from ecl.rep_fin_ecl_prep_base_data_ss_m prep_base
    left join default_info 
    on prep_base.loan_no=default_info.loan_no
    where prep_base.pt_date = '{{dt}}'

    union all

    select
    last_day(prep_base.loan_disbursement_date) 												as pt_date
    --,prep_base.pt_date																				as position_date
    ,nvl(months_between(pt_date,last_day(prep_base.loan_disbursement_date)),0)		as max_period		
    ,case when default_info_cif.first_default_date_client is null then 0
    else months_between(pt_date,default_info_cif.first_default_date_client)	end		as max_period_default				
    ,prep_base.loan_no																		as loan_no
    ,prep_base.client_no																	as client_no
    ,prep_base.product_code																	as prod_code
    ,prep_base.product_type																	as prod_desc
    ,prep_base.tenor																		as tenor
    ,prep_base.collectability_client														as collectability
    ,nvl(default_info_cif.first_default_date_client,'')												as pd_cohort_first_default_date
    ---client_no level as ecl only have DB client, if any account is default then the pd_cohort_default_flag_client will be 'Y' for all the loan_no under same client. 
    ,nvl(case when default_info_cif.first_default_date_client is not null then 'Y' end,'N')			as pd_cohort_default_flag
    ,prep_base.write_off_date																as write_off_date  
    ,prep_base.int_real_rate																as int_real_rate
    ,concat(prep_base.IMPORT_SOURCE,"_", prep_base.ec_number)								as src_ec
    ,CASE WHEN PREP_BASE.EC_NUMBER = 'EC1' and PREP_BASE.ecl_bucket_client <>'5' and PREP_BASE.day_past_due_client<'90' and default_info_cif.first_default_date_client is null THEN '0_Active'
    WHEN PREP_BASE.EC_NUMBER = 'EC3' THEN '2_Settled'
    WHEN PREP_BASE.EC_NUMBER = 'EC2' and default_info_cif.first_default_date_client is not null THEN '3_WO'
    else '1_Default' end 																	as condition
    ,prep_base.default_flag_client															as wo_flag
    ,prep_base.ecl_bucket_client															as ecl_bucket
    ,prep_base.cur_balance																	as cur_balance
    ,'client level' 																		as import_source
    ,'EC2'																				 	as ec_number													
    from ecl.rep_fin_ecl_prep_base_data_ss_m prep_base
    left join default_info_cif
    on prep_base.client_no=default_info_cif.client_no
    where prep_base.pt_date = '{{dt}}'
)
, base_population as (
    select pt_date, loan_no, client_no, cast(concat(prod_code, '_', tenor, '_', int_real_rate, '_', substring(m0_date,1,7)) as string) as cluster_id
    , prod_code, prod_desc, tenor, int_real_rate, m0_date
    , condition, ec_number, collectability, ecl_bucket, default_flag
    , count(loan_no) count_loan, count(distinct client_no) count_client, sum(cur_balance) cur_balance
    from prep_base_data
    where pt_date = last_day(pt_date)
    and prod_desc = 'SPL' and cast(tenor as int) = 24
    group by pt_date, loan_no, client_no, prod_code, prod_desc, tenor, int_real_rate, m0_date, condition, ec_number, collectability, ecl_bucket, default_flag
)
, active_population as (
    select 
        pt_date as snapshot_date
        , add_months(pt_date, 5 - ecl_bucket) as cut_off_date
        , prod_code
        , prod_desc
        , tenor
        , int_real_rate
        , condition
        , ec_number
        , ecl_bucket
        , default_flag
        , condition
        , count(loan_no) as count_loan
        , count(distinct client_no) as count_client
        , sum(cur_balance) as cur_balance
    from base_population
    where pt_date = last_day(pt_date)
    and condition in ('0_Active', '1_Default')
    group by pt_date, add_months(pt_date, 5 - ecl_bucket), prod_code, prod_desc, tenor, int_real_rate, condition, ec_number, ecl_bucket, default_flag, condition
)
, tracker as (
    select 
        A.pt_date as snapshot_date
        , A.prod_code
        , A.prod_desc
        , A.tenor
        , A.int_real_rate
        , A.condition
        , A.ec_number
        , A.ecl_bucket
        , B.pt_date as mt_date
        , months_between(B.pt_date, A.pt_date) + 1 as period
        , B.condition as mt_condition
        , B.ecl_bucket as mt_ecl_bucket
        , case when B.ecl_bucket = 5 then 1 else 0 end as mt_default_flag
        , max(case when B.ecl_bucket = 5 then 1 else 0 end) over (partition by A.pt_date, A.prod_code, A.prod_desc, A.tenor, A.int_real_rate, A.condition, A.ec_number, A.ecl_bucket order by A.pt_date, A.prod_code, A.prod_desc, A.tenor, A.int_real_rate, A.condition, A.ec_number, A.ecl_bucket, B.pt_date rows unbounded preceding) as mt_ever_default_flag
        , count(A.loan_no) as count_loan
        , count(distinct A.client_no) as count_client
        , sum(A.cur_balance) as cur_balance
    from base_population A, base_population B
    where B.pt_date >= A.pt_date
    and A.loan_no = B.loan_no
    and A.ec_number = B.ec_number
    and A.condition in ('0_Active', '1_Default') and B.condition <> '2_Settled'
    group by A.pt_date, A.prod_code, A.prod_desc, A.tenor, A.int_real_rate, A.condition, 
    A.ec_number, A.ecl_bucket, B.pt_date, months_between(A.pt_date, B.pt_date), B.condition, B.ecl_bucket, B.default_flag
)
, cohort_observation as (
    select B.*
    , A.mt_date
    , A.period
    , A.mt_condition
    , A.mt_ecl_bucket
    , A.mt_default_flag
    , A.mt_ever_default_flag
    , A.count_loan
    , A.count_client
    , A.cur_balance
    from tracker a
    left join active_population b
    on a.snapshot_date = b.snapshot_date
    and a.prod_code = b.prod_code 
    and a.prod_desc = b.prod_desc 
    and a.tenor = b.tenor 
    and a.int_real_rate = b.int_real_rate 
    and a.ec_number = b.ec_number
    and a.ecl_bucket = b.ecl_bucket
    and a.condition = b.condition
)
, cohort_odr as (
    select a.ec_number
    , a.tenor
    , a.ecl_bucket
    , a.mt_date
    , a.pd_segment
    , a.period
    , a.count_loan
    , a.count_client
    , a.cur_balance
    , a.mt_count_loan
    , a.mt_count_client
    , a.mt_cur_balance
    , a.mt_count_loan - lag(a.mt_count_loan, 1,0) over(partition by ec_number, tenor, pd_segment, ecl_bucket, count_loan, count_client, cur_balance, pt_date order by pt_date, pd_segment, tenor, mt_date, period asc) marginal_count_loan
    , a.mt_count_client - lag(a.mt_count_client, 1,0) over(partition by ec_number,tenor, pd_segment, ecl_bucket, count_loan, count_client, cur_balance, pt_date order by pt_date, pd_segment, tenor, mt_date, period asc) marginal_count_client
    , a.mt_cur_balance - lag(a.mt_cur_balance, 1,0) over(partition by ec_number,tenor, pd_segment, ecl_bucket, count_loan, count_client, cur_balance, pt_date order by pt_date, pd_segment, tenor, mt_date, period asc) marginal_cur_balance
    , (a.mt_count_loan - lag(a.mt_count_loan, 1,0) over(partition by ec_number, tenor, pd_segment, ecl_bucket, count_loan, count_client, cur_balance, pt_date order by pt_date, pd_segment, tenor, mt_date, period asc)) / a.count_loan as marginal_odr_loan
    , (a.mt_count_client - lag(a.mt_count_client, 1,0) over(partition by ec_number,tenor, pd_segment, ecl_bucket, count_loan, count_client, cur_balance, pt_date order by pt_date, pd_segment, tenor, mt_date, period asc)) / a.count_client as marginal_odr_client
    , (a.mt_cur_balance - lag(a.mt_cur_balance, 1,0) over(partition by ec_number,tenor, pd_segment, ecl_bucket, count_loan, count_client, cur_balance, pt_date order by pt_date, pd_segment, tenor, mt_date, period asc)) / a.cur_balance as marginal_odr_cur_balance
    , a.mt_count_loan / a.count_loan as cumulative_odr_loan
    , a.mt_count_client / a.count_client as cumulative_odr_client
    , a.mt_cur_balance / a.cur_balance cumulative_odr_cur_balance
    , a.cut_off_date
    , a.pt_date
    from (    
        select 
        B.ec_number
        , B.cut_off_date
        , cast(B.tenor as int) as tenor
        , cast(B.ecl_bucket as int) as ecl_bucket
        , A.mt_date
        , concat('Digital_', B.prod_desc) as pd_segment
        , A.period
        , B.count_loan
        , B.count_client
        , B.cur_balance
        , sum(A.count_loan) as mt_count_loan
        , sum(A.count_client) as mt_count_client
        , sum(A.cur_balance) as mt_cur_balance
        , B.snapshot_date as pt_date
        from tracker a
        left join (select B.snapshot_date, B.cut_off_date, B.ec_number, B.tenor, B.ecl_bucket, B.prod_desc, sum(B.count_loan) count_loan, sum(B.count_client) count_client, sum(B.cur_balance) cur_balance from active_population b group by B.snapshot_date, B.cut_off_date, B.ec_number, B.tenor, B.ecl_bucket,B.prod_desc) b
        on a.snapshot_date = b.snapshot_date 
        and a.prod_desc = b.prod_desc 
        and a.tenor = b.tenor 
        and a.ec_number = b.ec_number
        and a.ecl_bucket = b.ecl_bucket
        where A.mt_ever_default_flag = 1
        group by B.ec_number, B.cut_off_date, B.tenor, B.ecl_bucket, A.mt_date, concat('Digital_', B.prod_desc), A.period, B.count_loan, B.count_client, B.cur_balance, B.snapshot_date) a
)
, cohort_pd as (
    select 
    ec_number
    , pd_segment
    , tenor 
    , period
    , ecl_bucket
    , sum(marginal_count_loan) / sum(count_loan) as marginal_pd_cohort_loan
    , sum(marginal_count_client) / sum(count_client) as marginal_pd_cohort_client
    , sum(marginal_cur_balance) / sum(cur_balance) as marginal_pd_cohort_cur_balance
    , sum(mt_count_loan) / sum(count_loan) as cumulative_pd_cohort_loan
    , sum(mt_count_client) / sum(count_client) as cumulative_pd_cohort_client
    , sum(mt_cur_balance) / sum(cur_balance) as cumulative_pd_cohort_cur_balance
    , '{{dt}}' as pt_date
    from cohort_odr
    where cut_off_date < '{{dt}}'
    group by ec_number
    , pd_segment
    , tenor 
    , period
    , ecl_bucket
)
select * from cohort_odr;