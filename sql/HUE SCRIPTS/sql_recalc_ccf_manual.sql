with 1st_Default as (
    select pt_date,
        product_Type,
        client_no,
        first_default_date,
        add_months(last_day(first_default_date), -6) sixmbefore_1st_default_date,
        sum(cur_balance) as cur_balance,
        sum (first_default_principal) as first_default_principal
    from ecl.rep_fin_ecl_prep_master_data_ss_m
    where 1 = 1
        and last_day(first_default_date) = pt_date
        and ec_number = 'EC1'
    group by pt_date,
        product_Type,
        client_no,
        first_default_date
),
master_data as(
    select pt_date,
        product_code,
        product_type,
        client_no,
        limit_usage_bucket,
        loan_limit,
        cur_balance,
        ecl_bucket_client
    from ecl.rep_fin_ecl_prep_master_data_ss_m
    where ec_number = 'EC1'
),
limit_6m_before_1st_default as (
    select b.pt_date,
        b.product_Type,
        a.client_no,
        b.first_default_date,
        b.sixmbefore_1st_default_date,
        limit_usage_bucket,
        sum(a.loan_limit) as loan_limit_before_Default,
        c.product_limit
    from master_data a
        inner join 1st_Default b on a.pt_date = b.sixmbefore_1st_default_date
        and a.product_Type = b.product_Type
        and a.client_no = b.client_no
        inner join dim.map_fin_reg_product_code c on a.product_code = c.product_code
    where a.pt_date <= c.end_date
        and a.pt_date >= c.start_date
    GROUP BY b.pt_date,
        b.product_Type,
        a.client_no,
        b.first_default_date,
        b.sixmbefore_1st_default_date,
        limit_usage_bucket,
        c.product_limit
),
population as (
    select b.pt_date,
        b.product_Type,
        b.client_no,
        b.first_default_date,
        b.sixmbefore_1st_default_date,
        b.first_default_principal as 1st_default_principal,
        sum(a.cur_balance) as 6m_before_1st_default_principal,
        c.loan_limit_before_Default,
        c.limit_usage_bucket,
        c.product_limit
    from master_data a
        inner join 1st_Default b on a.pt_date = b.sixmbefore_1st_default_date
        and a.product_Type = b.product_Type
        and a.client_no = b.client_no
        inner join limit_6m_before_1st_default c on b.client_no = c.client_no
        AND c.product_Type = b.product_Type
        and b.pt_date = c.pt_date
    where a.ecl_bucket_client = 1
    group by b.pt_date,
        b.product_Type,
        b.client_no,
        b.first_default_date,
        b.sixmbefore_1st_default_date,
        b.first_default_principal,
        c.loan_limit_before_Default,
        c.limit_usage_bucket,
        c.product_limit
),
population2 as (
    select a.pt_date as pt_date,
        a.product_Type as product_type,
        a.client_no as client_no,
        a.first_default_date as fdd,
        a.1st_default_principal as fdp,
        a.6m_before_1st_default_principal as fdp_6m,
        a.loan_limit_before_Default as loan_limit,
        a.limit_usage_bucket as loan_limit_bucket,
        (
            a.1st_default_principal - a.6m_before_1st_default_principal
        ) as loan_increase,
        (
            a.loan_limit_before_Default - a.6m_before_1st_default_principal
        ) as undrawn_6m_before_1st_default,
case
            when (
                a.1st_default_principal - a.6m_before_1st_default_principal
            ) / (
                a.loan_limit_before_Default - a.6m_before_1st_default_principal
            ) < 0 then 0
            when (
                a.1st_default_principal - a.6m_before_1st_default_principal
            ) / (
                a.loan_limit_before_Default - a.6m_before_1st_default_principal
            ) > 1 then 1
            else (
                a.1st_default_principal - a.6m_before_1st_default_principal
            ) / (
                a.loan_limit_before_Default - a.6m_before_1st_default_principal
            )
        end as ccf,
case
            when (
                a.1st_default_principal - a.6m_before_1st_default_principal
            ) / (
                a.product_limit - a.6m_before_1st_default_principal
            ) < 0 then 0
            when (
                a.1st_default_principal - a.6m_before_1st_default_principal
            ) / (
                a.product_limit - a.6m_before_1st_default_principal
            ) > 1 then 1
            else (
                a.1st_default_principal - a.6m_before_1st_default_principal
            ) / (
                a.product_limit - a.6m_before_1st_default_principal
            )
        end as ccf2
    from population a
    order by a.product_Type,
        a.pt_date,
        a.limit_usage_bucket
),
ccf_result as (
    select b.pt_Date,
        a.product_Type,
        a.loan_limit_bucket,
        avg(a.ccf) as ccf,
        avg(a.ccf2) as ccf2
    from population2 a
        inner join (
            select distinct pt_Date
            from population2
        ) b
    where a.pt_date <= b.pt_Date
    group by b.pt_Date,
        a.product_Type,
        a.loan_limit_bucket
    order by b.pt_Date,
        a.product_Type,
        a.loan_limit_bucket
) population_final as (
    select pt_date,
        product_type,
        loan_limit_bucket,
        count(client_no) as count_loan,
        sum(fdp) as fdp
    from population2
    group by pt_date,
        product_type,
        loan_limit_bucket
)
select *
from ccf_result;