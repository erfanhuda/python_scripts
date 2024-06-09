with loan_default_cohort as (
    select pt_date,
        loan_no,
        client_no,
        product_code,
        product_type,
        tenor,
        collectability_client,
        ecl_bucket_client,
        cur_balance
    from ecl.rep_fin_ecl_prep_master_data_ss_m
    where (
            (
                ec_number = 'EC1'
                AND day_past_due_client > 90
            )
            OR ec_number = 'EC2'
        )
        and pt_date <= last_day('${date}')
),
loan_active_cohort as (
    select pt_date,
        loan_no,
        client_no,
        product_code,
        product_type,
        tenor,
        collectability_client,
        ecl_bucket_client,
        cur_balance
    from ecl.rep_fin_ecl_prep_master_data_ss_m
    where ec_number <= 'EC1'
        and pt_date = last_day('${date}')
),
count_active_cohort as (
    select pt_date,
        tenor,
        product_type,
        ecl_bucket_client,
        count(loan_no) count_noa,
        sum(cur_balance) as total_os_active
    from loan_active_cohort
    where pt_date <= last_day('${date}')
    group by pt_date,
        tenor,
        product_type,
        ecl_bucket_client
),
populate_account as (
    select a.pt_date,
        b.pt_date as date_for_default,
        a.tenor,
        a.product_type,
        a.ecl_bucket_client,
        sum(b.cur_balance) as os_default,
        count(b.loan_no) as count_default
    from loan_active_cohort a
        join loan_default_cohort b on a.loan_no = b.loan_no
    where a.pt_date <= b.pt_date
    group by a.pt_date,
        b.pt_date,
        a.tenor,
        a.product_type,
        a.ecl_bucket_client
)
select a.pt_date,
    b.date_for_default,
    case
        when a.pt_date = b.date_for_default then '1'
        when add_months(a.pt_date, 1) = b.date_for_default then '2'
        when add_months(a.pt_date, 2) = b.date_for_default then '3'
        when add_months(a.pt_date, 3) = b.date_for_default then '4'
        when add_months(a.pt_date, 4) = b.date_for_default then '5'
        when add_months(a.pt_date, 5) = b.date_for_default then '6'
        when add_months(a.pt_date, 6) = b.date_for_default then '7'
        when add_months(a.pt_date, 7) = b.date_for_default then '8'
        when add_months(a.pt_date, 8) = b.date_for_default then '9'
        when add_months(a.pt_date, 9) = b.date_for_default then '10'
        when add_months(a.pt_date, 10) = b.date_for_default then '11'
        when add_months(a.pt_date, 11) = b.date_for_default then '12'
        when add_months(a.pt_date, 12) = b.date_for_default then '13'
        when add_months(a.pt_date, 13) = b.date_for_default then '14'
        when add_months(a.pt_date, 14) = b.date_for_default then '15'
        when add_months(a.pt_date, 15) = b.date_for_default then '16'
        when add_months(a.pt_date, 16) = b.date_for_default then '17'
        when add_months(a.pt_date, 17) = b.date_for_default then '18'
        when add_months(a.pt_date, 18) = b.date_for_default then '19'
        when add_months(a.pt_date, 19) = b.date_for_default then '20'
        when add_months(a.pt_date, 20) = b.date_for_default then '21'
        when add_months(a.pt_date, 21) = b.date_for_default then '22'
        when add_months(a.pt_date, 22) = b.date_for_default then '23'
        when add_months(a.pt_date, 23) = b.date_for_default then '24'
        when add_months(a.pt_date, 24) = b.date_for_default then '25'
        when add_months(a.pt_date, 25) = b.date_for_default then '26'
        when add_months(a.pt_date, 26) = b.date_for_default then '27'
        when add_months(a.pt_date, 27) = b.date_for_default then '28'
        when add_months(a.pt_date, 28) = b.date_for_default then '29'
        when add_months(a.pt_date, 29) = b.date_for_default then '30'
        when add_months(a.pt_date, 30) = b.date_for_default then '31'
        when add_months(a.pt_date, 31) = b.date_for_default then '32'
        when add_months(a.pt_date, 32) = b.date_for_default then '33'
        when add_months(a.pt_date, 33) = b.date_for_default then '34'
        when add_months(a.pt_date, 34) = b.date_for_default then '35'
        when add_months(a.pt_date, 35) = b.date_for_default then '36'
        when add_months(a.pt_date, 36) = b.date_for_default then '37'
        when add_months(a.pt_date, 37) = b.date_for_default then '38'
        when add_months(a.pt_date, 38) = b.date_for_default then '39'
    end month_seq,
    a.tenor,
    a.product_type,
    a.ecl_bucket_client,
    a.count_noa,
    a.total_os_active,
    b.count_default,
    b.os_default
from count_active_cohort a
    left join populate_account b on a.pt_date = b.pt_date
    and a.tenor = b.tenor
    and a.product_type = b.product_type
    and a.ecl_bucket_client = b.ecl_bucket_client