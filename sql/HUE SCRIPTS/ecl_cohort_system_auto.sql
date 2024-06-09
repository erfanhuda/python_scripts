with base_population as (
    select pt_date,
        loan_no,
        client_no,
        cast(
            concat(
                prod_code,
                '_',
                tenor,
                '_',
                int_real_rate,
                '_',
                substring(m0_date, 1, 7)
            ) as string
        ) as cluster_id,
        prod_code,
        prod_desc,
        tenor,
        int_real_rate,
        m0_date,
        condition,
        ec_number,
        collectability,
        ecl_bucket,
        default_flag,
        count(loan_no) count_loan,
        count(distinct client_no) count_client,
        sum(cur_balance) cur_balance
    from ecl.rep_fin_ecl_pd_cohort_base_t_ss_m
    where pt_date = last_day(pt_date)
    group by pt_date,
        loan_no,
        client_no,
        prod_code,
        prod_desc,
        tenor,
        int_real_rate,
        m0_date,
        condition,
        ec_number,
        collectability,
        ecl_bucket,
        default_flag
),
active_population as (
    select pt_date as snapshot_date,
        add_months(pt_date, 5 - cast(ecl_bucket as int)) as cut_off_date,
        prod_code,
        prod_desc,
        tenor,
        int_real_rate,
        ec_number,
        ecl_bucket,
        default_flag,
        condition,
        count(loan_no) as count_loan,
        count(distinct client_no) as count_client,
        sum(cur_balance) as cur_balance
    from base_population
    where pt_date = last_day(pt_date)
        and condition in ('0_Active', '1_Default')
    group by pt_date,
        add_months(pt_date, 5 - cast(ecl_bucket as int)),
        prod_code,
        prod_desc,
        tenor,
        int_real_rate,
        condition,
        ec_number,
        ecl_bucket,
        default_flag,
        condition
),
period_series as (
    select add_months(last_day('${start_date}}'), a.pos) as mt_date,
        a.pos + 1 as period,
        b.snapshot_date,
        b.cut_off_date,
        b.prod_code,
        b.prod_desc,
        b.tenor,
        b.int_real_rate,
        b.condition,
        b.ec_number,
        b.ecl_bucket,
        b.count_loan,
        b.count_client,
        b.cur_balance
    from (
            select posexplode(
                    split(
                        repeat(
                            "o",
                            cast(
                                months_between(
                                    cast('${dt}' as date),
                                    cast('${start_date}' as date)
                                ) as int
                            )
                        ),
                        "o"
                    )
                )
        ) a
        cross join active_population b
),
tracker as (
    select A.pt_date as snapshot_date,
        A.prod_code,
        A.prod_desc,
        A.tenor,
        A.int_real_rate,
        A.condition,
        A.ec_number,
        A.ecl_bucket,
        B.pt_date as mt_date,
        months_between(B.pt_date, A.pt_date) + 1 as period,
        B.condition as mt_condition,
        B.ecl_bucket as mt_ecl_bucket,
        case
            when B.ecl_bucket = 5 then 1
            else 0
        end as mt_default_flag,
        max(
            case
                when B.ecl_bucket = 5 then 1
                else 0
            end
        ) over (
            partition by A.pt_date,
            A.prod_code,
            A.prod_desc,
            A.tenor,
            A.int_real_rate,
            A.condition,
            A.ec_number,
            A.ecl_bucket
            order by A.pt_date,
                A.prod_code,
                A.prod_desc,
                A.tenor,
                A.int_real_rate,
                A.condition,
                A.ec_number,
                A.ecl_bucket,
                B.pt_date rows unbounded preceding
        ) as mt_ever_default_flag,
        count(A.loan_no) as count_loan,
        count(distinct A.client_no) as count_client,
        sum(A.cur_balance) as cur_balance
    from base_population A,
        base_population B
    where B.pt_date >= A.pt_date
        and A.loan_no = B.loan_no
        and A.ec_number = B.ec_number
        and A.condition in ('0_Active', '1_Default')
        and B.condition <> '2_Settled'
    group by A.pt_date,
        A.prod_code,
        A.prod_desc,
        A.tenor,
        A.int_real_rate,
        A.condition,
        A.ec_number,
        A.ecl_bucket,
        B.pt_date,
        months_between(A.pt_date, B.pt_date),
        B.condition,
        B.ecl_bucket,
        B.default_flag
),
cohort_observation as (
    select B.*,
        A.mt_date,
        A.period,
        A.mt_condition,
        A.mt_ecl_bucket,
        A.mt_default_flag,
        A.mt_ever_default_flag,
        A.count_loan,
        A.count_client,
        A.cur_balance,
        max(A.count_loan) over (
            partition by B.ec_number,
            B.snapshot_date,
            B.prod_code,
            B.prod_desc,
            B.tenor,
            B.condition,
            B.ecl_bucket,
            A.period
            order by B.snapshot_date,
                B.prod_desc,
                B.tenor,
                B.ecl_bucket
        ),
        max(A.count_client) over (
            partition by B.ec_number,
            B.snapshot_date,
            B.prod_code,
            B.prod_desc,
            B.tenor,
            B.condition,
            B.ecl_bucket,
            A.period
            order by B.snapshot_date,
                B.prod_desc,
                B.tenor,
                B.ecl_bucket
        ),
        max(A.cur_balance) over (
            partition by B.ec_number,
            B.snapshot_date,
            B.prod_code,
            B.prod_desc,
            B.tenor,
            B.condition,
            B.ecl_bucket,
            A.period
            order by B.snapshot_date,
                B.prod_desc,
                B.tenor,
                B.ecl_bucket
        )
    from period_series B
        left join tracker A on a.snapshot_date = b.snapshot_date
        and a.prod_code = b.prod_code
        and a.prod_desc = b.prod_desc
        and a.tenor = b.tenor
        and a.int_real_rate = b.int_real_rate
        and a.ec_number = b.ec_number
        and a.ecl_bucket = b.ecl_bucket
        and a.condition = b.condition
        and A.period = B.period
),
cohort_odr as (
    select a.ec_number,
        a.tenor,
        a.ecl_bucket,
        a.mt_date,
        a.pd_segment,
        a.period,
        a.count_loan,
        a.count_client,
        a.cur_balance,
        a.mt_count_loan,
        a.mt_count_client,
        a.mt_cur_balance,
        a.mt_count_loan - lag(a.mt_count_loan, 1, 0) over(
            partition by ec_number,
            tenor,
            pd_segment,
            ecl_bucket,
            count_loan,
            count_client,
            cur_balance,
            pt_date
            order by pt_date,
                pd_segment,
                tenor,
                mt_date,
                period asc
        ) marginal_count_loan,
        a.mt_count_client - lag(a.mt_count_client, 1, 0) over(
            partition by ec_number,
            tenor,
            pd_segment,
            ecl_bucket,
            count_loan,
            count_client,
            cur_balance,
            pt_date
            order by pt_date,
                pd_segment,
                tenor,
                mt_date,
                period asc
        ) marginal_count_client,
        a.mt_cur_balance - lag(a.mt_cur_balance, 1, 0) over(
            partition by ec_number,
            tenor,
            pd_segment,
            ecl_bucket,
            count_loan,
            count_client,
            cur_balance,
            pt_date
            order by pt_date,
                pd_segment,
                tenor,
                mt_date,
                period asc
        ) marginal_cur_balance,
        if(
            a.mt_count_loan - lag(a.mt_count_loan, 1, 0) over(
                partition by ec_number,
                tenor,
                pd_segment,
                ecl_bucket,
                count_loan,
                count_client,
                cur_balance,
                pt_date
                order by pt_date,
                    pd_segment,
                    tenor,
                    mt_date,
                    period asc
            ) < 0,
            0,
            a.mt_count_loan - lag(a.mt_count_loan, 1, 0) over(
                partition by ec_number,
                tenor,
                pd_segment,
                ecl_bucket,
                count_loan,
                count_client,
                cur_balance,
                pt_date
                order by pt_date,
                    pd_segment,
                    tenor,
                    mt_date,
                    period asc
            )
        ) / a.count_loan as marginal_odr_loan,
        if(
            a.mt_count_client - lag(a.mt_count_client, 1, 0) over(
                partition by ec_number,
                tenor,
                pd_segment,
                ecl_bucket,
                count_loan,
                count_client,
                cur_balance,
                pt_date
                order by pt_date,
                    pd_segment,
                    tenor,
                    mt_date,
                    period asc
            ) < 0,
            0,
            a.mt_count_client - lag(a.mt_count_client, 1, 0) over(
                partition by ec_number,
                tenor,
                pd_segment,
                ecl_bucket,
                count_loan,
                count_client,
                cur_balance,
                pt_date
                order by pt_date,
                    pd_segment,
                    tenor,
                    mt_date,
                    period asc
            )
        ) / a.count_client as marginal_odr_client,
        if(
            a.mt_cur_balance - lag(a.mt_cur_balance, 1, 0) over(
                partition by ec_number,
                tenor,
                pd_segment,
                ecl_bucket,
                count_loan,
                count_client,
                cur_balance,
                pt_date
                order by pt_date,
                    pd_segment,
                    tenor,
                    mt_date,
                    period asc
            ) < 0,
            0,
            a.mt_cur_balance - lag(a.mt_cur_balance, 1, 0) over(
                partition by ec_number,
                tenor,
                pd_segment,
                ecl_bucket,
                count_loan,
                count_client,
                cur_balance,
                pt_date
                order by pt_date,
                    pd_segment,
                    tenor,
                    mt_date,
                    period asc
            )
        ) / a.cur_balance as marginal_odr_cur_balance,
        a.mt_count_loan / a.count_loan as cumulative_odr_loan,
        a.mt_count_client / a.count_client as cumulative_odr_client,
        a.mt_cur_balance / a.cur_balance cumulative_odr_cur_balance,
        a.cut_off_date,
        a.pt_date
    from (
            select B.ec_number,
                B.cut_off_date,
                cast(B.tenor as int) as tenor,
                cast(B.ecl_bucket as int) as ecl_bucket,
                A.mt_date,
                concat('Digital_', B.prod_desc) as pd_segment,
                A.period,
                B.count_loan,
                B.count_client,
                B.cur_balance,
                sum(A.count_loan) as mt_count_loan,
                sum(A.count_client) as mt_count_client,
                sum(A.cur_balance) as mt_cur_balance,
                B.snapshot_date as pt_date
            from tracker a
                left join (
                    select B.snapshot_date,
                        B.cut_off_date,
                        B.ec_number,
                        B.tenor,
                        B.ecl_bucket,
                        B.prod_desc,
                        sum(B.count_loan) count_loan,
                        sum(B.count_client) count_client,
                        sum(B.cur_balance) cur_balance
                    from active_population b
                    group by B.snapshot_date,
                        B.cut_off_date,
                        B.ec_number,
                        B.tenor,
                        B.ecl_bucket,
                        B.prod_desc
                ) b on a.snapshot_date = b.snapshot_date
                and a.prod_desc = b.prod_desc
                and a.tenor = b.tenor
                and a.ec_number = b.ec_number
                and a.ecl_bucket = b.ecl_bucket
            where A.mt_ever_default_flag = 1
            group by B.ec_number,
                B.cut_off_date,
                B.tenor,
                B.ecl_bucket,
                A.mt_date,
                concat('Digital_', B.prod_desc),
                A.period,
                B.count_loan,
                B.count_client,
                B.cur_balance,
                B.snapshot_date
        ) a
),
cohort_pd as (
    select ec_number,
        pd_segment,
        tenor,
        period,
        ecl_bucket,
        avg(marginal_odr_loan) as avg_marginal_pd_loan,
        avg(marginal_odr_client) as avg_marginal_pd_client,
        avg(marginal_odr_cur_balance) as avg_marginal_pd_balance,
        sum(
            if(marginal_count_loan < 0, 0, marginal_count_loan)
        ) / sum(count_loan) as wa_marginal_pd_loan,
        sum(
            if(
                marginal_count_client < 0,
                0,
                marginal_count_client
            )
        ) / sum(count_client) as wa_marginal_pd_client,
        sum(
            if(
                marginal_cur_balance < 0,
                0,
                marginal_cur_balance
            )
        ) / sum(cur_balance) as wa_marginal_pd_balance,
        '${dt}' as pt_date
    from cohort_odr
    where cut_off_date < '${dt}'
    group by ec_number,
        pd_segment,
        tenor,
        period,
        ecl_bucket
)
select *
from cohort_pd;