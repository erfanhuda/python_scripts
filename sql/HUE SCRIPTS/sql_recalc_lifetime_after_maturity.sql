with com_mk as (
    SELECT loan_no,
        pd_segment,
        loan_maturity_date,
        pt_date,
        CASE
            WHEN ec_number = 'EC2' THEN write_off_date
            WHEN ec_number = 'EC3' THEN settle_date
        END close_date
    FROM rep_fin_ecl_prep_master_data_ss_m
    WHERE pt_date in ("${pt_date}")
),
after_maturity_population as (
    SELECT *,
        datediff(close_date, loan_maturity_date) as lifetime_days,
        months_between(close_date, loan_maturity_date) as lifetime_m
    FROM com_mk
    where loan_maturity_date < close_date
        AND close_date IS NOT NULL
),
after_maturity_result as (
    select pt_date,
        pd_segment,
        count(loan_no) as count_loans,
        AVG(lifetime_days) as avg_lifetime_extension_days,
        STDDEV(lifetime_days) as stdev_lifetime_extension_days,
        AVG(lifetime_m) as avg_lifetime_extension_m,
        STDDEV(lifetime_m) as stdev_lifetime_extension_m
    from after_maturity_population
    group by pt_date,
        pd_segment
)
select *
from after_maturity_result;