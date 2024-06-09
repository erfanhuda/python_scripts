select *
from ecl.rep_fin_ecl_lgd_param_output_ss_m
where pt_date = '2023-09-30';
select distinct pd_segment
from ecl.rep_fin_ecl_pd_param_output_ss_m
where pt_date = '2024-01-31';
select *
from ecl.rep_fin_ecl_ccf_param_output_ss_m_merge
where pt_date = '2024-01-31';
select *
from ecl.rep_fin_ecl_pd_param_output_ss_m_merge
where pt_date = '2024-01-31';
select *
from ecl.rep_fin_ecl_lifetime_param_output_ss_m
where pt_date = '2024-01-31';
select *
from ecl.rep_fin_ecl_tms_master_rev_repo_ss_m
where pt_date = '2024-02-29';
select sum(
        unamortisation_premium_discount_idr + capital_accounting_idr + interest_receivable_idr
    )
from dm.rep_fin_reg_db_master_sec_agg_ss_d
where pt_date = '2024-01-31'
    and issuer_type <> 'Governments';
select pt_date,
    deal_no,
    first_leg_ccy,
    total_interest_accrual_ccy,
    interest_accrual_ccy,
    total_interest_accrued_idr,
    interest_accrual_idr,
,
    security_type
from dm.rep_fin_reg_db_master_repo_agg_ss_d
where pt_date = '2024-01-31'
    and deal_no in (2620, 2621);
select distinct product_code,
    product_type,
    tenor
from ecl.rep_fin_ecl_prep_base_data_ss_m
where pt_date = '2024-01-31';
select distinct product_code,
    product_type,
    tenor,
    tenor_in_month,
    payment_freq
from dm.rep_fin_reg_com_master_kredit_ss_d
where pt_date = '2024-02-25';
select *
from ecl.rep_fin_ecl_tms_master_placement_ss_m_merge
where pt_date = '2024-01-31';
select *
from ecl.rep_fin_ecl_tms_master_mm_ss_m
where pt_date = '2024-01-31';
select *
from ecl.rep_fin_ecl_tms_master_rev_repo_ss_m
where pt_date = '2024-01-31';
select *
from ecl.rep_fin_ecl_tms_master_securities_ss_m
where pt_date = '2024-02-29';
select *
from ecl.rep_fin_ecl_prep_base_data_ss_m
where pt_date = '2024-01-31';
select *
from ecl.rep_fin_ecl_tms_summary_output_ss_m
where pt_date = '2024-01-31';
select *
from ecl.rep_fin_ecl_tms_loss_rate_ss_m
where pt_date = '2024-01-31';
select *
from dim.map_fin_ecl_master_config;
select *
from ecl.rep_fin_ecl_tms_pd_base_ss_m
where pt_date = '2024-01-31'
    and rating = 'AA';
select *
from ecl.rep_fin_ecl_tms_pd_best_ss_m
where pt_date = '2023-12-31'
    and rating = 'AA'
select *
from ecl.rep_fin_ecl_tms_pd_worst_ss_m
where pt_date = '2023-12-31'
    and rating = 'AA';
select *
from ecl.rep_fin_ecl_tms_cpd_base_ss_m
where rating = 'AA';
select *
from ecl.rep_fin_ecl_tms_mev_hist_stats_qoq_ss_m
where pt_date = '2023-12-31';
select *
from dim.map_fin_ecl_tms_mev_data_input;
select *
from dim.map_fin_ecl_tms_ttcpd;
select *
from dim.map_fin_ecl_master_config;
select *
from dim.map_fin_ecl_tms_rating
where last_modified_date = '2024-02-20';
select *
from dm.rep_fin_reg_db_master_repo_agg_ss_d;
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
    sum(ecl_final) as ecl_final,
    sum(lifetime_ecl_drawn) as ecl_drawn,
    sum(ecl_final_principal) as ecl_final_principal,
    sum(ecl_final_interest) as ecl_final_interest,
    sum(lifetime_ecl_undrawn) as lifetime_ecl_undrawn
from ecl.rep_fin_ecl_ecl_summary_output_ss_m
where pt_date = '2024-02-29'
group by pt_date,
    tenor,
    ecl_bucket_client,
    collectability_client,
    product_code,
    substring(loan_disbursement_date, 1, 7),
    int_real_rate;
select *
from ecl.rep_fin_ecl_pd_param_output_ss_m_merge
where pt_date = '2024-01-31'
    and pd_segment = 'Digital_EAL'
    and tenor = 6
    and period = 1
    and ecl_bucket_client = 1;
select b.pt_date,
    b.tenor,
    b.ecl_bucket_client,
    b.product_code,
    b.period,
    b.int_real_rate,
    b.pd_base,
    b.lgd,
    b.discount_rate,
    a.loan_disbursement_date,
    count(b.loan_no) as count_loan,
    sum(b.cur_balance) as cur_balance,
    sum(b.accrued_interest_pl) as accrued_interest_pl,
    sum(b.ead_drawn) as ead_drawn,
    sum(b.ead_undrawn) as ead_undrawn,
    sum(b.ecl_final) as ecl_final,
    sum(b.ecl_drawn_weighted) as ecl_drawn,
    sum(b.ecl_undrawn_weighted) as lifetime_ecl_undrawn
from ecl.rep_fin_ecl_ecl_term_structure_ss_m b
    left join dm.rep_fin_reg_com_master_kredit_ss_d a on b.pt_date = a.pt_date
    and a.loan_no = b.loan_no
where b.pt_date = '2024-02-29'
    and b.period = 1
    and b.product_code = 103
    and b.tenor = 3
group by b.pt_date,
    b.tenor,
    b.ecl_bucket_client,
    b.product_code,
    b.period,
    b.int_real_rate,
    b.pd_base,
    b.lgd,
    b.discount_rate,
    a.loan_disbursement_date;
select *
from ecl.rep_fin_ecl_ecl_term_structure_ss_m
where pt_date = '2024-02-29'
    and pd_segment = 'Digital_SPL'
    and tenor = 1
    and ecl_bucket_client = 4
    and product_code = 101;
select *
from ecl.rep_fin_ecl_lifetime_param_output_ss_m_merge
where pt_date = '2024-01-31'
    and pd_segment = 'Digital_BCL'
    and tenor = 3;
select *
from ecl.rep_fin_ecl_pd_param_output_ss_m_merge
where pt_date = '2024-01-31'
    and pd_segment = 'Digital_SPL'
    and tenor = 12
    and period = 12
    and ecl_bucket_client = 1;
select *
from ecl.rep_fin_ecl_lgd_param_output_ss_m_merge
where pt_date = '2024-01-31'
    and pd_segment = 'Digital_SPL';
select *
from ecl.rep_fin_ecl_ecl_summary_output_ss_m
where pt_date = '2024-01-31';
select distinct pt_date,
    prod_type
from dm.rep_db_mid_loan_receipt_tab_ss_d
where pt_date between '2024-01-01' and '2024-03-03'
    and settle_type = 'BUYBACK'
    and pt_date = concat(
        substring(settle_date, 1, 4),
        "-",
        substring(settle_date, 5, 2),
        "-",
        substring(settle_date, 7, 2)
    );
select *
from ecl.rep_fin_ecl_lifetime_param_output_ss_m
where pt_date = '2024-01-31';
select *
from dim.map_fin_ecl_multiplier
where product_type = 'SPL'
    and start_date = '2024-01-01';
SELECT product_code,
    product_type,
    tenor,
    ecl_bucket_client,
    loan_disbursement_date,
    loan_maturity_date,
    tenor_remaining,
    lifetime,
    sum(ecl_final)
FROM ECL.rep_fin_ecl_ecl_summary_output_ss_m
where pt_date = '2024-02-29'
    and loan_maturity_date >= '2024-02-29'
    and pd_segment = 'Digital_SPL'
group by product_code,
    product_type,
    tenor,
    ecl_bucket_client,
    tenor_remaining,
    lifetime,
    loan_disbursement_date,
    loan_maturity_date;
select pt_date,
    product_type,
    tenor,
    sum(principal) principal,
    sum(principal_mtd) principal_mtd
from id_finpro.flow_rate_base_disburse
where pt_date between '${start_date}' and '${end_date}'
group by pt_date,
    product_type,
    tenor;