with com_mk as (
    select min(pt_date) as pt_date,
        loan_no,
        stage,
        loan_disbursement_date -- added by Erfan
    from dm.rep_fin_reg_com_master_kredit_ss_d
    where day_past_due_client = "${sicr_dpd_rule}" -- modified
        and import_source = 'Digibank'
    group by loan_no,
        stage,
        loan_disbursement_date
),
stage2 as (
    select a.pt_date,
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
    where a.pt_date = "${pt_date}"
        and a.ec_number in('EC2', 'EC3') --and case when a.settle_date is null or a.settle_Date='' then a.write_off_date else a.settle_Date end = null 
        --GROUP BY a.pt_Date, a.loan_no,a.settle_date,a.product_type,a.tenor,a.ec_number,a.write_off_date,a.settle_date ,a.import_source
),
behavioural_accounts as (
    select a.pt_date,
        a.ec_number,
        a.import_source,
        a.loan_no,
        a.product_type,
        a.tenor,
        b.pt_date AS STAGE2_DATE,
        b.loan_disbursement_date,
        -- added by Erfan
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
        ) as close_to_pt_d,
        months_between(
            CASE
                WHEN ec_number = 'EC2' THEN write_off_date
                WHEN ec_number = 'EC3' THEN settle_date
            END,
            b.pt_date
        ) as close_to_pt_m,
        datediff(
            b.loan_disbursement_date,
            CASE
                WHEN ec_number = 'EC2' THEN write_off_date
                WHEN ec_number = 'EC3' THEN settle_date
            END
        ) as disb_to_close_d,
        -- close date (in days)
        months_between(
            b.loan_disbursement_date,
            CASE
                WHEN ec_number = 'EC2' THEN write_off_date
                WHEN ec_number = 'EC3' THEN settle_date
            END
        ) as disb_to_close_m,
        -- close date (in months)
        datediff(b.loan_disbursement_date, b.pt_date) as disb_to_pt_d,
        --behave date (in days)
        months_between(b.loan_disbursement_date, b.pt_date) as disb_to_pt_m -- behave date (in months)
    from ecl.rep_fin_ecl_prep_master_data_ss_m a
        inner join com_mk b on a.loan_no = b.loan_no
    where a.pt_date = "${pt_date}"
        and a.ec_number in ('EC2', 'EC3')
),
lifetime_count as (
    select pt_date,
        product_type,
        tenor,
        lifetime_days,
        lifetime_m,
        count(loan_no) as count_loan,
        avg(lifetime_Days) as avg_average_lifetime_days,
        STDDEV(lifetime_Days) as stdev_lifetime,
        avg(lifetime_m) as avg_average_lifetime_m,
        STDDEV(lifetime_m) as stdev_lifetime_m
    from stage2
    group by pt_date,
        product_type,
        tenor,
        lifetime_days,
        lifetime_m
),
actual_lifetime as (
    SELECT pt_date,
        product_type,
        tenor,
        disb_to_pt_m,
        close_to_pt_m,
        disb_to_close_m,
        round(disb_to_close_m, 0) - round(disb_to_pt_m, 0) as extension,
        count(loan_no) as count_loan
    FROM behavioural_accounts
    group by pt_date,
        product_type,
        tenor,
        disb_to_pt_m,
        close_to_pt_m,
        disb_to_close_m,
        round(disb_to_close_m, 0) - round(disb_to_pt_m, 0)
),
pull_data as (
    select pt_date,
        product_type,
        tenor,
        abs(extension),
        sum(count_loan)
    from id_finpro.ecl_actual_cumulative_lifetime
    group by pt_date,
        product_type,
        tenor,
        extension
)
SELECT *
FROM actual_lifetime;