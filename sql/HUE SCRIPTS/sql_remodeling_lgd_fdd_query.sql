with mk_marginal_recovery as (
    select pt_date,
        mom_date,
        pd_segment,
        tenor,
        ecl_bucket_client,
        ecl_bucket_client_mom,
        int_real_rate,
        first_default_date,
        last_day(first_default_date) as first_default_month,
        months_between(pt_date, last_day(first_default_date)) as period,
        case
            when datediff(mom_date, first_default_date) > 30
            and first_default_date < "2022-10-31" then 1
            when datediff(mom_date, first_default_date) > 90
            and first_default_date >= "2022-10-31" then 1
            else 0
        end as mom_flag_status,
        datediff(mom_date, first_default_date) as fdd_mom_days,
        case
            when datediff(pt_date, first_default_date) > 30
            and first_default_date < "2022-10-31" then 1
            when datediff(pt_date, first_default_date) > 90
            and first_default_date >= "2022-10-31" then 1
            else 0
        end as pt_flag_status,
        datediff(pt_date, first_default_date) as fdd_pt_days,
        case
            when datediff(pt_date, first_default_date) <= 30 then "90+"
            when (datediff(pt_date, first_default_date) > 30)
            and (datediff(pt_date, first_default_date) <= 90) then "120+"
            when datediff(pt_date, first_default_date) > 90 then "180+"
            else "0"
        end as group_pt_days,
        count(loan_no) as count_loan,
        count(distinct client_no) as count_client,
        sum(cur_balance_mom) as os_mom,
        sum(cur_balance) as os,
        sum(first_default_principal) as fdp,
        sum(recovery_amount) as marginal_rec_amount,
        sum(pv_recovery_amount) as marginal_pv_rec_amt
    from ecl.rep_fin_ecl_lgd_recovery_analysis_ss_m
    where pt_date <= "${working_month_end}"
        and first_default_date <= add_months("${working_month_end}", -6)
    group by pt_date,
        mom_date,
        pd_segment,
        tenor,
        ecl_bucket_client,
        ecl_bucket_client_mom,
        int_real_rate,
        first_default_date,
        case
            when datediff(mom_date, first_default_date) > 30
            and first_default_date < "2022-10-31" then 1
            when datediff(mom_date, first_default_date) > 90
            and first_default_date >= "2022-10-31" then 1
            else 0
        end,
        datediff(mom_date, first_default_date),
        case
            when datediff(pt_date, first_default_date) > 30
            and first_default_date < "2022-10-31" then 1
            when datediff(pt_date, first_default_date) > 90
            and first_default_date >= "2022-10-31" then 1
            else 0
        end,
        datediff(pt_date, first_default_date),
        case
            when datediff(pt_date, first_default_date) <= 30 then "90+"
            when (datediff(pt_date, first_default_date) > 30)
            and (datediff(pt_date, first_default_date) <= 90) then "120+"
            when datediff(pt_date, first_default_date) > 90 then "180+"
            else "0"
        end
),
batch_month as (
    select first_default_month as first_default_date,
        pd_segment,
        pt_flag_status,
        group_pt_days,
        avg(int_real_rate) as int_real_rate,
        sum(fdp) as fdp,
        sum(count_loan) as count_loan,
        sum(count_client) as count_client
    from mk_marginal_recovery
    where period = 0
    group by first_default_month,
        pd_segment,
        pt_flag_status,
        group_pt_days
),
recovery_marginal as (
    select first_default_month as first_default_date,
        pd_segment,
        pt_flag_status,
        group_pt_days,
        period,
        sum(marginal_rec_amount) as marginal_rec_amount,
        sum(marginal_pv_rec_amt) as marginal_pv_rec_amount
    from mk_marginal_recovery
    group by first_default_month,
        pd_segment,
        pt_flag_status,
        period,
        group_pt_days
),
summary_batch_result as (
    select b.first_default_date,
        b.pd_segment,
        b.int_real_rate,
        b.count_loan,
        b.count_client,
        b.fdp,
        a.pt_flag_status,
        a.group_pt_days,
        a.period,
        nvl(a.marginal_rec_amount, 0),
        nvl(a.marginal_pv_rec_amount, 0),
        nvl(a.marginal_rec_amount, 0) / b.fdp as marginal_rec_rate,
        nvl(a.marginal_pv_rec_amount, 0) / b.fdp as marginal_pv_rec_rate
    from batch_month b
        join recovery_marginal a on a.first_default_date = b.first_default_date
        and a.pd_segment = b.pd_segment
),
recovery_rate_seg_reconciliation as (
    select pt_date,
        pd_segment,
        lgd_segment,
        avg(recovery_rate_loan) as avg_rate_loan,
        sum(recovery_rate_loan) as sum_rate_loan,
        count(loan_no) as count_loan,
        count(distinct loan_no) as count_distinct_loan,
        sum(recovery_rate_loan) / count(distinct loan_no) as final_recovery_rate_seg
    from ecl.rep_fin_ecl_lgd_recovery_rate_loan_ss_m
    group by pt_date,
        pd_segment,
        lgd_segment
)
select *
from summary_batch_result;