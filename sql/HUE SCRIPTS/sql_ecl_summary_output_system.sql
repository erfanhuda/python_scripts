-- Final ECL System Output
select sum(ecl_final)
from (
        select pt_date,
            product_code,
            pd_segment,
            lgd_segment,
            product_type,
            collectability_acct,
            collectability_client,
            ecl_bucket_acct,
            ecl_bucket_client,
            ecl_stage_acct,
            ecl_stage_client,
            int_real_rate,
            coll_indiv_flag,
            loan_disbursement_date,
            loan_maturity_date,
            lifetime,
            expected_life,
            limit_usage_bucket,
            count(loan_no) as loan_no,
            count(distinct client_no) as client_no,
            sum(cur_balance) as cur_balance,
            sum(accrued_interest_pl) as accrued_interest_pl,
            sum(gca) as gca,
            sum(loan_limit) as loan_limit,
            sum(loan_undrawn) as loan_undrawn,
            sum(ecl_drawn_12m) as ecl_drawn_12m,
            sum(lifetime_ecl_drawn) as lifetime_ecl_drawn,
            sum(ecl_undrawn_12m) as ecl_undrawn_12m,
            sum(lifetime_ecl_undrawn) as lifetime_ecl_undrawn,
            sum(ecl_final_12m) as ecl_final_12m,
            sum(lifetime_ecl_final) as lifetime_ecl_final,
            sum(ecl_final_principal) as ecl_final_principal,
            sum(ecl_final_interest) as ecl_final_interest,
            sum(ecl_final) as ecl_final
        from ecl.rep_fin_ecl_ecl_summary_output_ss_m
        where pt_date = "${pt_date}"
        group by pt_date,
            product_code,
            pd_segment,
            lgd_segment,
            product_type,
            collectability_acct,
            collectability_client,
            ecl_bucket_acct,
            ecl_bucket_client,
            ecl_stage_acct,
            ecl_stage_client,
            int_real_rate,
            coll_indiv_flag,
            loan_disbursement_date,
            loan_maturity_date,
            lifetime,
            expected_life,
            limit_usage_bucket
    ) a
where a.product_code in ('101', '102', '103', '104', '105');
-- Term Structure
select distinct pt_date,
    product_code,
    product_type,
    ecl_bucket_client,
    period,
    tenor_remaining,
    lifetime,
    pd_base,
    pd_best,
    pd_worst,
    pd_weighted,
    lgd
from ecl.rep_fin_ecl_ecl_term_structure_ss_m
where pt_date = "${pt_date}" -- ECL Parameter Upload Check
select distinct "PD" as table_name,
    pd_segment
from ecl.rep_fin_ecl_pd_param_output_ss_m_merge
where pt_date = "${pt_date}"
union all
select distinct "CCF",
    pd_segment
from ecl.rep_fin_ecl_ccf_param_output_ss_m_merge
where pt_date = "${pt_date}"
union all
select distinct "LGD",
    pd_segment
from ecl.rep_fin_ecl_lgd_param_output_ss_m_merge
where pt_date = "${pt_date}"
union all
select distinct "LIFETIME",
    pd_segment
from ecl.rep_fin_ecl_lifetime_param_output_ss_m_merge
where pt_date = "${pt_date}";
select pt_date,
    sum(cur_balance),
    sum(ecl_final)
from ecl.rep_fin_ecl_ecl_summary_output_ss_m
where product_code not in ('101', '102', '103', '104', '105')
group by pt_date;
select pt_date,
    case
        when product_type IN ('RCL', 'AKL', 'EAL') then case
            when tenor_in_month is not null then tenor_in_month
            else round(
                months_between(A.loan_maturity_date, A.loan_disbursement_date),
                0
            )
        end
        else tenor
    end as tenor,
    ecl_bucket,
    max_ecl as ecl_bucket_cif,
    day_past_due_client,
    stage,
    max_col as col_cif,
    restructure,
    restru_date,
    restru_freq,
    product_code,
    substring (a.loan_disbursement_date, 1, 7) as disburse_month,
    a.int_real_rate,
    count(1) as loan_no,
    count(distinct c.client_no) as cif,
    sum(cur_balance) as outstanding_principal,
    sum(total_int_accrued) as outstanding_interest,
    sum(total_int_accrued_adj) as total_int_accrued_adj,
    sum(fac_amount) as fac_amount,
    sum(c.lifetime_undrawn) as ecl_undrawn,
    sum(int_income) as int_income,
    sum(c.ecl_final_principal) as ecl_principal,
    sum(c.ecl_final_interest) as ecl_interest,
    sum(c.ecl_final) as ecl_amount
from (
        select *
        from dm.rep_fin_reg_com_master_kredit_ss_d
        where pt_date = "${pt_date}"
            and (
                import_source = 'Digibank'
                or product_code in('C01', 'C02', 'M05', 'SC1', 'SC2')
            )
    ) as a
    join (
        select b.client_no,
            max (cast (b.ecl_bucket as numeric)) as max_ecl,
            max (cast (b.day_past_due as numeric)) as max_dpd,
            max (cast (b.collectability as numeric)) as max_col,
            sum(b.lifetime_ecl_undrawn) lifetime_undrawn,
            sum(b.ecl_undrawn_12m) 12m_undrawn,
            sum(b.ecl_final_principal) as ecl_final_principal,
            sum(b.ecl_final_interest) as ecl_final_interest,
            sum(b.ecl_final) as ecl_final
        from dm.rep_fin_reg_com_master_kredit_ss_m as b
            left join dm.rep_fin_reg_com_master_kredit_ss_d as c on b.loan_no = c.loan_no
            and b.pt_date = c.pt_date
        where b.pt_date = "${pt_date}"
        group by b.client_no
    ) as c on (a.client_no = c.client_no)
group by pt_date,
    case
        when product_type IN ('RCL', 'AKL', 'EAL') then case
            when tenor_in_month is not null then tenor_in_month
            else round(
                months_between(A.loan_maturity_date, A.loan_disbursement_date),
                0
            )
        end
        else tenor
    end,
    ecl_bucket,
    max_ecl,
    day_past_due_client,
    stage,
    max_col,
    restructure,
    restru_date,
    restru_freq,
    product_code,
    substring (a.loan_disbursement_date, 1, 7),
    a.int_real_rate;
select pt_date,
    tenor,
    ecl_bucket_client,
    product_code,
    product_type,
    sum(cur_balance) as cur_balance,
    sum(accrued_interest) as accrued_interest,
    sum(accrued_interest_pl) as accrued_interest_pl,
    sum(ead_drawn) as ead_drawn,
    sum(ead_undrawn) ead_undrawn,
    sum(ecl_final) as ecl_final
from ecl.rep_fin_ecl_ecl_term_structure_ss_m
where pt_date = '2024-01-31'
group by pt_date,
    product_code,
    product_type;
/* GENERATE REPORT FOR TERM STRUCTURE applied by system for LOAN*/
select pt_date,
    tenor,
    ecl_bucket_client,
    collectability_client,
    product_code,
    substring(loan_disbursement_date, 1, 7) as disb_month,
    int_real_rate,
    count(loan_no) as count_loan,
    sum(cur_balance) as cur_balance,
    sum(accrued_interest_pl) as accrued_interest_pl,
    sum(gca) as gca,
    sum(loan_undrawn) as fac_amount,
    sum(ecl_final_principal) as ecl_final_principal,
    sum(ecl_final_interest) as ecl_final_interest,
    sum(lifetime_ecl_drawn) as lifetime_ecl_drawn,
    sum(lifetime_ecl_undrawn) as lifetime_ecl_undrawn,
    sum(ecl_final) as ecl_final
from ecl.rep_fin_ecl_ecl_summary_output_ss_m
where pt_date = '${pt_date}'
group by pt_date,
    tenor,
    ecl_bucket_client,
    collectability_client,
    product_code,
    substring(loan_disbursement_date, 1, 7),
    int_real_rate;
/* GENERATE REPORT FOR TERM STRUCTURE applied for TREASURY*/
select *
from ecl.REP_FIN_ECL_TMS_SUMMARY_OUTPUT_SS_M
where pt_date = '${pt_date}';
/* GENERATE SCENARIO WEIGHTED FOR WP ECL SYSTEM */
select *
from dim.map_fin_ecl_master_config
where function_key in ('RATIO_BASE', 'RATIO_BEST', 'RATIO_WORST');
/* GENERATE PD TS FL FOR WP ECL SYSTEM */
with pd_scenario as (
    select pd_segment,
        tenor,
        ecl_bucket_client,
        period,
        collect_set(scenario ['BASE']) [0] as MPD_M_Base,
        collect_set(scenario ['BEST']) [0] as MPD_M_Best,
        collect_set(scenario ['WORST']) [0] as MPD_M_Worst
    from (
            select pd_segment,
                tenor,
                ecl_bucket_client,
                period,
                map(
                    scenario,
                    case
                        when scenario = 'BASE' then pd_base
                        when scenario = 'BEST' then pd_best
                        when scenario = 'WORST' then pd_worst
                    end
                ) as scenario
            from ecl.rep_fin_ecl_pd_param_output_ss_m_merge
            where pt_date = add_months('${pt_date}', -1)
                and pk_grouping_id <> ''
        ) a
    group by pd_segment,
        tenor,
        ecl_bucket_client,
        period
),
weighted as (
    select pt_date,
        collect_set(map_key ['RATIO_BASE']) [0] as RATIO_BASE,
        collect_set(map_key ['RATIO_BEST']) [0] as RATIO_BEST,
        collect_set(map_key ['RATIO_WORST']) [0] as RATIO_WORST
    from (
            select map(function_key, value) as map_key,
                '${pt_date}' as pt_date
            from dim.map_fin_ecl_master_config
            where function_key in ('RATIO_BASE', 'RATIO_BEST', 'RATIO_WORST')
        ) a
    group by pt_date
)
select pd_segment,
    tenor,
    ecl_bucket_client,
    period,
    mpd_m_base,
    sum(mpd_m_base) over (
        partition by pd_segment,
        tenor,
        ecl_bucket_client
        order by pd_segment,
            tenor,
            ecl_bucket_client,
            period
    ) as CPD_M_BASE,
    mpd_m_best,
    sum(mpd_m_best) over (
        partition by pd_segment,
        tenor,
        ecl_bucket_client
        order by pd_segment,
            tenor,
            ecl_bucket_client,
            period
    ) as CPD_M_BEST,
    mpd_m_worst as mpd_m_worst,
    sum(mpd_m_worst) over (
        partition by pd_segment,
        tenor,
        ecl_bucket_client
        order by pd_segment,
            tenor,
            ecl_bucket_client,
            period
    ) as CPD_M_WORST,
    (mpd_m_base * b.ratio_base) + (mpd_m_best * b.ratio_best) + (mpd_m_worst * b.ratio_worst) as mpd_weighted,
    (
        sum(mpd_m_base) over (
            partition by pd_segment,
            tenor,
            ecl_bucket_client
            order by pd_segment,
                tenor,
                ecl_bucket_client,
                period
        ) * b.ratio_base
    ) + (
        sum(mpd_m_best) over (
            partition by pd_segment,
            tenor,
            ecl_bucket_client
            order by pd_segment,
                tenor,
                ecl_bucket_client,
                period
        ) * b.ratio_best
    ) + (
        sum(mpd_m_worst) over (
            partition by pd_segment,
            tenor,
            ecl_bucket_client
            order by pd_segment,
                tenor,
                ecl_bucket_client,
                period
        ) * b.ratio_worst
    ) as cpd_weighted
from pd_scenario
    cross join weighted b on pt_date = b.pt_date
order by pd_segment,
    tenor,
    ecl_bucket_client,
    period;
/* GENERATE LGD FOR WP ECL SYSTEM */
select add_months(pt_date, 1) as pt_date,
    lgd_segment,
    split(pd_segment, "_") [1] as pd_segment,
    pd_segment as ecl_segment,
    recovery_rate,
    lgd as lgd_month
from ecl.rep_fin_ecl_lgd_param_output_ss_m_merge
where pt_date = add_months('${pt_date}', -1);
/* GENERATE CCF FOR WP ECL SYSTEM */
select add_months(pt_date, 1) as pt_date,
    split(pd_segment, "_") [1] as pd_segment,
    limit_usage_ratio_bucket,
    ccf
from ecl.rep_fin_ecl_ccf_param_output_ss_m_merge
where pt_date = add_months('${pt_date}', -1);
/* GENERATE WP FL FROM SYSTEM */
with mk_closing as (
    select pt_date,
        loan_no,
        client_no,
        group_client_no,
        nvl(tenor_in_month, tenor) as tenor_in_month,
        ecl_bucket,
        ecl_bucket_max,
        col,
        product_code,
        substring (loan_disbursement_date, 1, 7) as disburse_month,
        count(loan_no) as count_loan,
        sum(cur_balance) as cur_balance,
        sum(
            case
                when total_int_accrued < 0 then 0
                else total_int_accrued
            end
        ) as total_int_accrued,
        sum(
            case
                when total_int_accrued_adj < 0 then 0
                else total_int_accrued_adj
            end
        ) as total_int_accrued_adj,
        sum(fac_amount) as fac_amount,
        int_real_rate
    from dm.rep_fin_reg_com_master_kredit_ss_d
    where pt_date = "${pt_date}"
        and (
            import_source = 'Digibank'
            or product_code in('C01', 'C02', 'M05', 'SC1', 'SC2')
        )
    group by pt_date,
        nvl(tenor_in_month, tenor),
        ecl_bucket,
        ecl_bucket_max,
        col,
        product_code,
        substring (loan_disbursement_date, 1, 7),
        int_real_rate,
        loan_no,
        client_no,
        group_client_no
)
select a.pt_date,
    a.tenor,
    b.ecl_bucket,
    a.ecl_bucket_client,
    b.col,
    a.product_code,
    b.disburse_month,
    count(b.loan_no) count_loan,
    sum(a.cur_balance) cur_balance,
    sum(a.accrued_interest) total_int_accrued,
    sum(b.total_int_accrued_adj) total_int_accrued_adj,
    sum(a.ead_undrawn) as ead_undrawn,
    a.int_real_rate * 100,
    b.tenor_in_month,
    case
        when a.ecl_bucket_client = 5 then 3
        when a.ecl_bucket_client < 3 then 1
        else 2
    end as stage_cif,
    sum(a.accrued_interest_pl) accrued_interest_pl,
    a.product_type,
    sum(a.ead_undrawn) / sum(a.ead_drawn + a.ead_undrawn) as utility_ratio,
    a.ccf,
    a.lgd,
    sum(a.pd_weighted) as pd_lifetime,
    sum(a.ecl_drawn_weighted) as ecl_principal_interest,
    sum(a.ecl_undrawn_weighted) as ecl_facility,
    sum(a.ecl_final) as ecl_total
from ecl.rep_fin_ecl_ecl_term_structure_ss_m a
    left join mk_closing as b on a.loan_no = b.loan_no
where a.pt_date = '${pt_date}'
group by a.pt_date,
    a.tenor,
    b.ecl_bucket,
    a.ecl_bucket_client,
    b.col,
    a.product_code,
    b.disburse_month,
    a.int_real_rate,
    case
        when a.ecl_bucket_client = 5 then 3
        when a.ecl_bucket_client < 3 then 1
        else 2
    end,
    a.product_type,
    a.ccf,
    b.tenor_in_month,
    a.lgd;
/* GENERATE RECONCILIATION OUTPUT ECL */
select a.pt_date,
    a.tenor,
    a.tenor_in_month,
    a.ecl_bucket,
    a.ecl_bucket_max,
    a.col,
    a.product_code,
    substring(a.loan_disbursement_date, 1, 7) as disb_month,
    a.product_type,
    a.int_real_rate,
    case
        when a.ecl_bucket_max = 5 then 3
        when a.ecl_bucket_max < 3 then 1
        else 2
    end as stage_cif,
    count(a.loan_no) count_loan,
    sum(a.cur_balance) cur_balance,
    sum(a.total_int_accrued) total_int_accrued,
    sum(a.loan_undrawn) as ead_undrawn,
    sum(a.total_int_accrued_adj) accrued_interest,
    sum(b.accrued_interest_pl) as accrued_interest_pl,
    sum(a.pd_lgd) as pd_lgd_lifetime,
    sum(a.ecl_final_principal) as ecl_principal_facility,
    sum(a.ecl_final_interest) as ecl_interest,
    sum(a.ecl_final) as ecl_total
from dm.rep_fin_reg_com_master_kredit_ss_m a
    left join ecl.rep_fin_ecl_ecl_summary_output_ss_m b on a.pt_date = b.pt_date
    and a.loan_no = b.loan_no
where a.pt_date = '${pt_date}'
group by a.pt_date,
    a.tenor,
    a.tenor_in_month,
    a.ecl_bucket,
    a.ecl_bucket_max,
    a.col,
    a.product_code,
    substring(a.loan_disbursement_date, 1, 7),
    a.product_type,
    a.int_real_rate,
    case
        when a.ecl_bucket_max = 5 then 3
        when a.ecl_bucket_max < 3 then 1
        else 2
    end;