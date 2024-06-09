with test as (
    select loan_no,
        client_no,
        pt_date as mt_date,
        ecl_bucket_acct,
        ecl_bucket_client,
        cur_balance,
        ec_number,
        collect_set(
            array(
                pt_date,
                cast(cur_balance as numeric),
                ecl_bucket_acct,
                ecl_bucket_client,
                ec_number,
                product_code,
                product_type,
                tenor,
                int_real_rate,
                collectability_acct
            )
        ) over (
            partition by loan_no
            order by pt_date
        ) as m0_population
    from ecl.rep_fin_ecl_prep_base_data_ss_m
    where pt_date between '2023-01-31' and '2023-12-31'
        and pt_date = last_day(pt_date)
        and loan_no in (
            '1000001147965444111006292',
            '1000003111222677558340832',
            '1000008442266947638150553',
            '1000011883877920820532752',
            '1000013258338770945587358',
            '1000020481735553105107004',
            '1000045894557933613266568',
            '1000063551088787554673702',
            '1000052368654897224525675',
            '1000054288991809556063276'
        )
);
select *,
    1 / power(
        (1 + int_real_rate),
        (payment_date - pt_date) / 360
    )
from ecl.rep_fin_ecl_repayment_schedule_ss_m
where pt_date = '2024-02-29'
    and pd_segment = 'Digital_BCL';
select distinct pd_segment
from ecl.rep_fin_ecl_repayment_schedule_ss_m
where pt_date = '2024-02-29'
select *,
    m0_lists.m0_data
from (
        select loan_no,
            client_no,
            pt_date as mt_date,
            ecl_bucket_acct,
            ecl_bucket_client,
            cur_balance,
            ec_number,
            collect_set(
                array(
                    pt_date,
                    product_code,
                    product_type,
                    tenor,
                    cast(cur_balance as numeric),
                    case
                        when ecl_bucket_acct = 5 then 1
                        else 0
                    end,
                    case
                        when ecl_bucket_client = 5 then 1
                        else 0
                    end,
                    ec_number
                )
            ) over (
                partition by loan_no,
                ec_number,
                pt_date
                order by pt_date
            ) as m0_population
        from ecl.rep_fin_ecl_prep_base_data_ss_m
        where pt_date = last_day(pt_date)
            and pt_date < '2022-12-31'
            and loan_no = '1000001147965444111006292'
    ) a lateral view explode(m0_population) m0_lists as m0_data
where ec_number = 'EC1';
select loan_no,
    client_no,
    pt_date as mt_date,
    ecl_bucket_acct,
    ecl_bucket_client,
    cur_balance,
    ec_number,
    collect_set(
        array(
            pt_date,
            cast(cur_balance as numeric),
            ecl_bucket_acct,
            ecl_bucket_client,
            ec_number,
            product_code,
            product_type,
            tenor,
            int_real_rate,
            collectability_acct
        )
    ) over (
        partition by loan_no,
        ec_number,
        pt_date
        order by pt_date
    ) as m0_population
from ecl.rep_fin_ecl_prep_base_data_ss_m
where loan_no in (
        '1000001147965444111006292',
        '1000013258338770945587358',
        '1000052368654897224525675'
    )
    and pt_date = last_day(pt_date);