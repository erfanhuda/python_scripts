with lgd_population as (
    select pt_date,
        mom_date,
        first_default_date,
        pd_segment,
        lgd_segment,
        tenor,
        count(loan_no) as count_loan,
        count(client_no) as count_client,
        sum(cur_balance) as os,
        sum(cur_balance_mom) as os_balance_mom,
        sum(first_default_principal) as fdp_amount,
        sum(recovery_amount) as recovery_amt,
        sum(pv_recovery_amount) as pv_recovery_amt,
        avg(int_real_rate) as int_real_rate
    from ecl.rep_fin_ecl_lgd_recovery_analysis_ss_m
    group by pt_date,
        mom_date,
        first_default_date,
        pd_segment,
        lgd_segment,
        tenor
),
lifetime_population as (
    with com_mk as (
        select min(pt_date) as pt_date,
            loan_no,
            stage
        from dm.rep_fin_reg_com_master_kredit_ss_d
        where ecl_bucket_max = "${bucket_sicr_rule}"
            and import_source = 'Digibank' --and product_type='SCL'
            --AND tenor=12
        group by loan_no,
            stage
    ),
    sicr_base_population as (
        select a.pt_Date,
            a.ec_number,
            a.import_source,
            a.loan_no,
            a.product_type,
            a.tenor,
            b.pt_date AS STAGE2_DATE,
            (
                CASE
                    WHEN ec_number = 'EC2' THEN write_off_date
                    WHEN ec_number = 'EC3' THEN settle_date
                END
            ) as close_date,
            datediff(
                CASE
                    WHEN ec_number = 'EC2' THEN write_off_date
                    WHEN ec_number = 'EC3' THEN settle_date
                END,
                b.pt_date
            ) as lifetime_days,
            months_between(
                CASE
                    WHEN ec_number = 'EC2' THEN write_off_date
                    WHEN ec_number = 'EC3' THEN settle_date
                END,
                b.pt_date
            ) as lifetime_m
        from ecl.rep_fin_ecl_prep_master_data_ss_m a
            inner join com_mk b on a.loan_no = b.loan_no
        where a.pt_date = "${pop_date}"
            and a.ec_number in('EC2', 'EC3')
    ),
    population as (
        select pt_date,
            product_type,
            tenor,
            close_date,
            lifetime_days,
            lifetime_m,
            count(loan_no) as count_loans
        from sicr_base_population
        group by pt_date,
            product_type,
            tenor,
            close_date,
            lifetime_days,
            lifetime_m
    ),
    lifetime_result as (
        select pt_date,
            product_type,
            tenor,
            count(loan_no) as count_loan,
            avg(lifetime_Days) as average_lifetime_days,
            STDDEV(lifetime_Days) as stdev_lifetime,
            avg(lifetime_m) as average_lifetime_m,
            STDDEV(lifetime_m) as stdev_lifetime_m
        from sicr_base_population
        group by pt_date,
            product_type,
            tenor
    )
    select *
    from population
),
tm_population(
    select "mom" as freq,
        *
    from ecl.rep_fin_ecl_pd_data_agg_mom_ss_m
    where mom_date <= "${pop_date}"
    select "qoq" as freq,
        *
    from ecl.rep_fin_ecl_pd_data_agg_qoq_ss_m
    where qoq_date <= "${pop_date}"
    select "hoh" as freq,
        *
    from ecl.rep_fin_ecl_pd_data_agg_hoh_ss_m
    where hoh_date <= "${pop_date}"
    select "yoy" as freq,
        *
    from ecl.rep_fin_ecl_pd_data_agg_yoy_ss_m
    where yoy_date <= "${pop_date}"
),
ttc_pd_population as (
    select "mom" as freq,
        *
    from ecl.rep_fin_ecl_pd_ttc_pd_mom_ss_m
    where pt_date <= "${pop_date}"
    UNION ALL
    select "qoq" as freq,
        *
    from ecl.rep_fin_ecl_pd_ttc_pd_qoq_ss_m
    where pt_date <= "${pop_date}"
    UNION ALL
    select "hoh" as freq,
        *
    from ecl.rep_fin_ecl_pd_ttc_pd_hoh_ss_m
    where pt_date <= "${pop_date}"
    UNION ALL
    select "yoy" as freq,
        *
    from ecl.rep_fin_ecl_pd_ttc_pd_yoy_ss_m
    where pt_date <= "${pop_date}"
),
pit_pd_population as(
    select "mom",
        *
    from ecl.rep_fin_ecl_pd_pit_pd_mom_ss_m
    where pt_date <= "${pop_date}"
    select "qoq",
        *
    from ecl.rep_fin_ecl_pd_pit_pd_qoq_ss_m
    where pt_date <= "${pop_date}"
    select "hoh",
        *
    from ecl.rep_fin_ecl_pd_pit_pd_hoh_ss_m
    where pt_date <= "${pop_date}"
    select "yoy",
        *
    from ecl.rep_fin_ecl_pd_pit_pd_yoy_ss_m
    where pt_date <= "${pop_date}"
)
select *
from lifetime_population;