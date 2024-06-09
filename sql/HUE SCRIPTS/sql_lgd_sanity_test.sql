with prep_master_population as (
    SELECT a.loan_no,
        a.first_default_date,
        a.first_default_principal,
        a.int_real_rate,
        a.product_type
    FROM ecl.rep_fin_ecl_prep_master_data_ss_m as a
    where pt_date <= last_day(add_months(last_day('${working_month_end}'), -6))
        and a.first_default_date <= last_day(add_months(last_day('${working_month_end}'), -6))
        and trim(a.first_default_date) <> ''
        and a.first_default_date is not null
    group by a.loan_no,
        a.first_default_date,
        a.first_default_principal,
        a.product_type,
        a.int_real_rate
),
prep_master_m_min1 as(
    select pt_date,
        loan_no,
        last_day(add_months(pt_date, -1)) as pt_date_prev,
        cur_balance,
        first_default_date,
        first_default_principal
    from ecl.rep_fin_ecl_prep_master_data_ss_m
),
prep_master_m as(
    select a.pt_date,
        a.pt_date_prev,
        a.loan_no,
        (
            case
                when substring(a.first_default_date, 1, 7) = substring(a.pt_date, 1, 7) then a.first_default_principal
                else b.cur_balance
            end
        ) - a.cur_balance as recovery
    from prep_master_m_min1 as a
        join ecl.rep_fin_ecl_prep_master_data_ss_m as b on (
            a.loan_no = b.loan_no
            and a.pt_date_prev = b.pt_date
        )
),
prep_master_comparison as (
    select a.loan_no,
        a.first_default_date,
        a.first_default_principal,
        a.int_real_rate,
        a.product_type,
        b.pt_date,
        case
            when b.recovery is null then a.first_default_principal
            else b.recovery
        end as recovery
    from prep_master_population as a
        left join prep_master_m as b on (a.loan_no = b.loan_no)
    where a.first_default_date < b.pt_date
        and b.pt_date <= last_day('${working_month_end}')
),
prep_master_pv as (
    select a.loan_no,
        a.first_default_date,
        a.first_default_principal,
        a.pt_date,
        a.recovery,
        a.product_type,
        a.recovery / power(
            (1 + (a.int_real_rate / 100) / 360),
            (datediff(a.pt_date, a.first_default_date) + 1)
        ) as pv_rec
    from prep_master_comparison as a
),
prep_master_pv_grp as (
    select a.loan_no,
        a.first_default_date,
        a.first_default_principal,
        a.product_type,
        sum(a.recovery) as rec_amount,
        sum(a.pv_rec) as pv_rec_amount,
        sum(a.pv_rec) / a.first_default_principal as pv_rec_rate
    from prep_master_pv as a
    group by a.loan_no,
        a.first_default_date,
        a.first_default_principal,
        a.product_type
),
prep_master_final as (
    select a.product_type,
        sum(a.first_default_principal) as first_default_principal,
        sum(a.pv_rec_amount) as pv_rec_amount,
        avg(pv_rec_rate) as avg_pv_rec_rate
    from prep_master_pv_grp as a
    group by a.product_type
),
prep_master_final2 as (
    select a.first_default_date as first_default_date,
        a.product_type as product_type,
        count(a.loan_no),
        sum(a.first_default_principal) as first_default_principal,
        sum(a.rec_amount) as rec_amount,
        sum(a.pv_rec_amount) as pv_rec_amount,
        avg(pv_rec_rate) as avg_pv_rec_rate
    from prep_master_pv_grp as a
    group by a.product_type,
        a.first_default_date
)
select *
from prep_master_comparison;
select "${working_month_end}",
    pd_segment,
    lgd_segment,
    sum(FIRST_DEFAULT_PRINCIPAL) as FIRST_DEFAULT_PRINCIPAL,
    sum(recovery_amount) as RECOVERY_AMOUNT,
    sum(PV_RECOVERY_AMOUNT) PV_RECOVERY_AMOUNT
from ecl.rep_fin_ecl_lgd_recovery_analysis_ss_m
where PT_DATE <= ADD_MONTHS(LAST_DAY('${working_month_end}'), -2) -- 2023-06-30 => 2023-04-30
    and months_between(
        ADD_MONTHS(LAST_DAY('${working_month_end}'), -1),
        first_default_date
    ) > 6 -- workout period ->  2023-06-30 => start: 2023-05-31, end: first_default_date
group by "${working_month_end}",
    pd_segment,
    lgd_segment,
    FIRST_DEFAULT_PRINCIPAL;
-- WORKOUT PERIOD
with population as (
    select loan_no,
        pd_segment,
        first_default_date,
        first_default_principal
    from rep_fin_ecl_lgd_recovery_analysis_ss_m
    where pt_date <= add_months(last_day('${working_month_end}'), -1)
    group by loan_no,
        pd_segment,
        first_default_date,
        first_default_principal
)
select '${date}' as pt_Date,
    a.pd_segment,
    date_format(a.first_default_date, "YYYY-MM") as fdm,
    round(
        months_between(
            add_months(last_day('${working_month_end}'), -1),
            b.first_default_date
        ),
        0
    ) as pv_month,
    count(a.loan_no),
    sum(a.first_default_principal),
    sum(b.recovery_amount),
    sum(b.pv_recovery_amount)
from population a
    left join rep_fin_ecl_lgd_recovery_analysis_ss_m b on a.loan_no = b.loan_no
where b.pt_date <= add_months(last_day('${working_month_end}'), -1)
    and months_between(
        add_months(last_day(b.pt_date), -1),
        b.first_default_date
    ) > 6
group by a.pd_segment,
    date_format(a.first_default_date, "YYYY-MM"),
    round(
        months_between(
            add_months(last_day('${date}'), -1),
            b.first_default_date
        ),
        0
    );
select a.pt_date,
    a.pd_segment,
    date_format(a.first_default_date, "YYYY-MM") as fdm,
    round(
        months_between(
            add_months(last_day(a.pt_date), -1),
            last_day(a.first_default_date)
        ),
        0
    ) as pv_month,
    count(a.loan_no),
    sum(a.first_default_principal) as fdp,
    sum(a.recovery_amount) as recovery,
    sum(a.pv_recovery_amount) as pv_recovery,
    row_number() over (
        partition by date_format(a.first_default_date, "YYYY-MM"),
        pd_segment
        order by pd_segment,
            date_format(a.first_default_date, "YYYY-MM")
    ) as mob
from rep_fin_ecl_lgd_recovery_analysis_ss_m a
where pt_date <= add_months('${working_month_end}', -1)
    and months_between(
        add_months(last_day(a.pt_date), -1),
        a.first_default_date
    ) > 6
group by a.pt_date,
    a.pd_segment,
    date_format(a.first_default_date, "YYYY-MM"),
    round(
        months_between(
            add_months(last_day(a.pt_date), -1),
            last_day(a.first_default_date)
        ),
        0
    );
select pt_date
from rep_fin_ecl_lgd_recovery_analysis_ss_m
where pd_segment = 'Digital_SPL'
    AND last_day(first_default_date) = '2021-10-31'
group by pt_date;
--------------------------------------------------------------------------------------------
create table id_finpro.rep_fin_ecl_lgd_recovery_rate_loan_ss_m as
select recovery_analysis.loan_no,
    recovery_analysis.client_no,
    recovery_analysis.pd_segment,
    recovery_analysis.lgd_segment,
    recovery_analysis.tenor,
    recovery_analysis.ecl_bucket_client,
    recovery_analysis.ecl_bucket_client_mom,
    recovery_analysis.FIRST_DEFAULT_PRINCIPAL,
    recovery_analysis.PV_RECOVERY_AMOUNT -- FIRST_DEFAULT_PRINCIPAL 最开始需要还款的额度  recovery_rate_loan < 1 还款额度 能够 覆盖多少
,
    recovery_analysis.PV_RECOVERY_AMOUNT / recovery_analysis.FIRST_DEFAULT_PRINCIPAL recovery_rate_loan,
    LAST_DAY(ADD_MONTHS('{dt}', -1)) pt_date
from (
        select loan_no,
            client_no,
            pd_segment,
            lgd_segment,
            tenor,
            ecl_bucket_client,
            ecl_bucket_client_mom,
            FIRST_DEFAULT_PRINCIPAL -- 所有还款的钱 的 present value
,
            sum(PV_RECOVERY_AMOUNT) PV_RECOVERY_AMOUNT
        from ecl.rep_fin_ecl_lgd_recovery_analysis_ss_m -- 取历史所有数据
        where PT_DATE <= LAST_DAY(ADD_MONTHS('2023-06-30', -2)) --3.30
            -- lgd 每个月最重recovery的数目
            -- 银行 贷款 default 追债 存在6个月后   5.8跑数据 4.30 -----> 10.31之前default loan
            and months_between(
                LAST_DAY(ADD_MONTHS('2023-06-30', -1)),
                first_default_date
            ) > 6
        group by loan_no,
            client_no,
            pd_segment,
            lgd_segment,
            tenor,
            ecl_bucket_client,
            ecl_bucket_client_mom,
            FIRST_DEFAULT_PRINCIPAL
    ) recovery_analysis;
--------------------------------------------------------------------------------------------
create table id_finpro.rep_fin_ecl_lgd_recovery_rate_segment_ss_m as
select pd_segment,
    lgd_segment -- secured 担保 unsecured
,
    sum(RECOVERY_RATE_LOAN) / count(distinct loan_no) RECOVERY_RATE_SEG,
    pt_date
from id_finpro.rep_fin_ecl_lgd_recovery_rate_loan_ss_m
where pt_date = LAST_DAY(ADD_MONTHS('2023-06-30', -1))
group by pd_segment,
    lgd_segment,
    pt_date;
--------------------------------------------------------------------------------------------
create table id_finpro.rep_fin_ecl_lgd_param_output_ss_m as
select lgd.pd_segment,
    lgd.lgd_segment,
    lgd.RECOVERY_RATE_SEG,
    config.recovery_cost_rate -- 1 - lgd 有可能损失的部分 + 比如催收、逾期 会产生费用 把这个比率加上
,
    (1 - lgd.RECOVERY_RATE_SEG) + config.recovery_cost_rate lgd,
    LAST_DAY(ADD_MONTHS('{dt}', -1)) as pt_date
from id_finpro.rep_fin_ecl_lgd_recovery_rate_segment_ss_m lgd
    cross join (
        select nvl(cast(value as double), 0) as recovery_cost_rate
        from dim.map_fin_ecl_master_config
        where function_key = 'DIRECT_COST'
            and start_date <= '{dt}'
            and end_date >= '{dt}'
    ) config
where lgd.pt_date = LAST_DAY(ADD_MONTHS('2023-06-30', -1));
----------------------------------------------------------------------------------------------
with prep_master_population as (
    SELECT a.loan_no,
        a.first_default_date,
        a.first_default_principal,
        a.int_real_rate,
        a.product_type
    FROM ecl.rep_fin_ecl_prep_master_data_ss_m as a
    where pt_date <= last_day(add_months(last_day('${working_month_end}'), -6))
        and pt_date = last_day(pt_date)
        and a.first_default_date <= last_day(add_months(last_day('${working_month_end}'), -6))
        and trim(a.first_default_date) <> ''
        and a.first_default_date is not null
    group by a.loan_no,
        a.first_default_date,
        a.first_default_principal,
        a.product_type,
        a.int_real_rate
),
prep_master_m_min1 as(
    select pt_date,
        loan_no,
        last_day(add_months(pt_date, -1)) as pt_date_prev,
        cur_balance,
        first_default_date,
        first_default_principal
    from ecl.rep_fin_ecl_prep_master_data_ss_m
    where pt_date = last_day(pt_Date)
),
prep_master_m as(
    select a.pt_date,
        a.pt_date_prev,
        a.loan_no,
        (
            case
                when substring(a.first_default_date, 1, 7) = substring(a.pt_date, 1, 7) then a.first_default_principal
                else b.cur_balance
            end
        ) - a.cur_balance as recovery
    from prep_master_m_min1 as a
        join ecl.rep_fin_ecl_prep_master_data_ss_m as b on (
            a.loan_no = b.loan_no
            and a.pt_date_prev = b.pt_date
        )
),
prep_master_comparison as (
    select a.loan_no,
        a.first_default_date,
        a.first_default_principal,
        a.int_real_rate,
        a.product_type,
        b.pt_date,
        case
            when b.recovery is null then a.first_default_principal
            else b.recovery
        end as recovery
    from prep_master_population as a
        left join prep_master_m as b on (a.loan_no = b.loan_no)
    where a.first_default_date < b.pt_date
        and b.pt_date <= last_day('${working_month_end}')
),
prep_master_pv as (
    select a.loan_no,
        a.first_default_date,
        a.first_default_principal,
        a.pt_date,
        a.recovery,
        a.product_type,
        recovery / power(
            (1 + (int_real_rate / 100) / 360),
            (datediff(pt_date, first_default_date) + 1)
        ) as pv_rec
    from prep_master_comparison as a
),
prep_master_pv_grp as (
    select a.loan_no,
        a.first_default_date,
        a.first_default_principal,
        a.product_type,
        sum(a.pv_rec) as pv_rec_amount,
        sum(a.pv_rec) / a.first_default_principal as pv_rec_rate
    from prep_master_pv as a
    group by a.loan_no,
        a.first_default_date,
        a.first_default_principal,
        a.product_type
),
recovery_rate_loan as (
    select sum(pv_recovery_amount) as pv_rec_rate_loan,
        loan_no,
        pd_segment
    from rep_fin_ecl_lgd_recovery_rate_loan_ss_m
    where pt_date = '${working_month_end}'
    group by loan_no,
        pd_segment
)
select a.loan_no,
    a.first_default_date,
    a.first_default_principal,
    a.product_type,
    pv_rec_amount,
    pv_rec_rate,
    b.pv_rec_rate_loan
from prep_master_pv_grp a
    inner join recovery_rate_loan b on a.loan_no = b.loan_no
where pv_rec_amount <> pv_rec_rate_loan ----------------------------------------------------------------------------------------------
select *
from ecl.rep_fin_ecl_lgd_recovery_rate_loan_ss_m
where loan_no in (
        "1000334295727243302964027",
        "1000643250806489147125434"
    )
    and pt_date > "2022-10-31";
select *
from ecl.rep_fin_ecl_lgd_recovery_analysis_ss_m
where loan_no in (
        "1000334295727243302964027",
        "1000643250806489147125434"
    )
    and pt_date >= "2022-06-30";
----------------------------------------------------------------------------------------------
select *
from ecl.rep_fin_ecl_lgd_param_output_ss_m
where pt_date = "2023-06-30";
--- REVERSE POPULATION ---
select first_default_date,
    period,
    pd_segment,
    count(distinct loan_no) as count_loan,
    avg(int_real_rate) as int_real_rate,
    count(distinct client_no) as count_client,
    sum(first_default_principal) as fdp,
    sum(a.rec_amt_segment) as rec_amt_segment,
    sum(a.rec_amt_loan) as rec_amt_loan,
    sum(a.rec_amt_seg) as cumulative_pv_rec_amt,
    sum(a.recovery_rate_loan) as cumulative_pv_rate_loan
from (
        select a.pt_date as pt_date,
            months_between(a.pt_date, last_day(b.first_default_date)) as period,
            a.pd_segment as pd_segment,
            last_day(b.first_default_date) as first_default_date,
            a.loan_no,
            a.client_no,
            a.first_default_principal,
            a.pv_recovery_amount as rec_amt_seg,
            a.recovery_rate_loan,
            b.recovery_amount as rec_amt_segment,
            b.recovery_amount as rec_amt_loan,
            b.int_real_rate as int_real_rate
        from ecl.rep_fin_ecl_lgd_recovery_rate_loan_ss_m a
            inner join (
                select pt_date,
                    loan_no,
                    first_default_date,
                    tenor,
                    int_real_rate,
                    sum(recovery_amount) as recovery_amount,
                    sum(pv_recovery_amount) as pv_recovery_amount
                from ecl.rep_fin_ecl_lgd_recovery_analysis_ss_m
                group by pt_date,
                    loan_no,
                    first_default_date,
                    tenor,
                    int_real_rate
            ) b on a.loan_no = b.loan_no
            and add_months(a.pt_date, -6) = b.pt_date
        where a.pt_date <= "${working_month_end}"
            and a.pd_segment = "Digital_SPL"
    ) a
group by first_default_date,
    pd_segment,
    period;