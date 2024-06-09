with com_mk as (
    select min(pt_date) as pt_date,
        loan_no,
        stage
    from dm.rep_fin_reg_com_master_kredit_ss_d
    where ecl_bucket = "${bucket_sicr_rule}"
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
    where a.pt_date = "${pt_date}"
        and a.ec_number in('EC2', 'EC3')
),
population as (
    select pt_date,
        product_type,
        tenor,
        close_date,
        round(lifetime_days, 0),
        round(lifetime_m, 0),
        count(loan_no) as count_loan
    from sicr_base_population
    group by pt_date,
        product_type,
        tenor,
        close_date,
        round(lifetime_days, 0),
        round(lifetime_m, 0)
),
manual_lifetime_result as (
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
from manual_lifetime_result;