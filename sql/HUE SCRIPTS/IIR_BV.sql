-- case 20231231:
-- Settled: 171211110, disb date: 	2023-09-25, settle_date: 20231018, disb amt: IDR 8027052871, prin_pay_schedule: , int_pay_schedule: IDR, header_pay_code: 10, 2209, 0, detail_pay_code: 
-- Active: 159001400, disb date: 	2023-09-07, disb amt: IDR 300.000.000, prin_pay_schedule: , int_pay_schedule: IDR , header_pay_code: 10, 2201, , detail_pay_code: 410, 420,
with accounts as (
    select pt_date,
        nvl(
            from_unixtime(
                unix_timestamp(cast(cast(lmdtnt as int) as string), 'yyyyMMdd'),
                'yyyy-MM-dd'
            ),
            ''
        ) as disb_date,
        nvl(
            last_day(
                from_unixtime(
                    unix_timestamp(cast(cast(lmdtnt as int) as string), 'yyyyMMdd'),
                    'yyyy-MM-dd'
                )
            ),
            ''
        ) as disb_month,
        lmnoac,
        lmcdpl,
        lmnotp,
        lmnofp,
        lmrtnm,
        lmcdst,
        lmdtpo,
        case
            when lmcdst = 1
            and lmnopd <= 90 then 'Active'
            when lmcdst = 1
            and lmnopd > 90 then 'NPL'
            when lmcdst = 2 then 'Settled'
            when lmcdst = 3 then 'Write Off'
        end as ec_status,
        sum(lmamab) as amortized_cost,
        (sum(lmamcb) + sum(lmamab)) - sum(lmamcl) as unamortized_cost,
        sum(lmamcb) as cur_balance,
        sum(lmamcl) as loan_disb
    from ods.bke_visiona_lmbal_ss
    where pt_date = '${end_date}'
        and lmcdpl in ('SC1', 'SC2', 'C01', 'M05')
    group by pt_date,
        lmnoac,
        lmcdpl,
        lmcdst,
        lmnotp,
        lmnofp,
        lmrtnm,
        lmdtpo,
        nvl(
            from_unixtime(
                unix_timestamp(cast(cast(lmdtnt as int) as string), 'yyyyMMdd'),
                'yyyy-MM-dd'
            ),
            ''
        ),
        nvl(
            last_day(
                from_unixtime(
                    unix_timestamp(cast(cast(lmdtnt as int) as string), 'yyyyMMdd'),
                    'yyyy-MM-dd'
                )
            ),
            ''
        ),
        case
            when lmcdst = 1
            and lmnopd <= 90 then 'Active'
            when lmcdst = 1
            and lmnopd > 90 then 'NPL'
            when lmcdst = 2 then 'Settled'
            when lmcdst = 3 then 'Write Off'
        end
),
pay_schedule as (
    -- select pt_date, l7noac, sum(nvl(l7ampr,0)) as principal_schedule, sum(nvl(l7amin,0)) as int_schedule 
    -- from ods.bke_visiona_lbillx_ss where pt_date = '${end_date}' and pt_date = last_day(pt_date)
    -- group by pt_date,l7noac
    select pt_date,
        l7noac,
        sum(l7ampr) as principal_schedule,
        sum(l7amin) as int_schedule
    from ods.bke_visiona_lbillx_ss
    where pt_date = '${end_date}'
    group by pt_date,
        l7noac
),
detail_disbursement_amount as (
    select a.pt_date,
        b.pt_date as new_pt,
        a.disb_date,
        a.disb_month,
        a.lmnoac,
        a.lmcdpl,
        a.lmcdst,
        a.ec_status,
        a.lmnotp,
        a.lmnofp,
        a.lmrtnm,
        a.lmdtpo,
        count(1) as count_loan,
        sum(a.cur_balance) as cur_balance,
        sum(a.amortized_cost) as amortized_cost,
        sum(a.unamortized_cost) as unamortized_cost,
        sum(a.loan_disb) as loan_disb,
        sum(b.principal_schedule) as prin_schedule,
        sum(b.int_schedule) as int_schedule
    from accounts a
        left join pay_schedule b on a.pt_date = b.pt_date
        and a.lmnoac = b.l7noac
    where a.disb_date <> nvl(
            from_unixtime(
                unix_timestamp(
                    cast(cast(a.lmdtpo as int) as string),
                    'yyyyMMdd'
                ),
                'yyyy-MM-dd'
            ),
            ''
        ) -- on  a.pt_date = b.pt_date and a.lmnoac = b.lbnoac
    group by a.pt_date,
        b.pt_date,
        a.disb_date,
        a.disb_month,
        a.lmnoac,
        a.lmcdpl,
        a.lmcdst,
        a.ec_status,
        a.lmnotp,
        a.lmnofp,
        a.lmrtnm,
        a.lmdtpo
),
actual_header_repaid as (
    select pt_date,
        lhnoac,
        lhnotc,
        max(lhdttr) as pay_date,
        sum(lhamtr) as pay_amount
    from ods.bke_visiona_lhish_ss
    where pt_date = "${end_date}"
    group by pt_date,
        lhnoac,
        lhnotc
),
actual_detail_repaid as (
    -- select pt_date, lhnoac, lhnotc, max(lhdttr) as pay_date, sum(lhamtr) as pay_amount 
    -- from ods.bke_visiona_lhisd_ss where pt_date <= "${end_date}" and pt_date = last_day(pt_date)
    -- group by pt_date,lhnoac, lhnotc
    select pt_date,
        lhnoac,
        lhnotc,
        cast(cast(lhnoac as int) as string) as account_no,
        date_format(pt_date, 'yyyy-MM') as report_month,
        concat (
            substring(cast(cast (lhdttr as int) as string), 1, 4),
            '-',
            substring(cast(cast (lhdttr as int) as string), 5, 2)
        ) as lhdttr_1,
        max(lhdttr) as pay_date,
        sum(lhamtr) as pay_amount
    from ods.bke_visiona_lhisd_ss
    where pt_date = '${end_date}'
        and pt_date = last_day(pt_date)
    group by pt_date,
        lhnoac,
        lhnotc,
        cast(cast(lhnoac as int) as string),
        date_format(pt_date, 'yyyy-MM'),
        concat (
            substring(cast(cast (lhdttr as int) as string), 1, 4),
            '-',
            substring(cast(cast (lhdttr as int) as string), 5, 2)
        )
),
final_actual_repayment as (
    select y.pt_date,
        y.loan_id,
        case
            when y.header_code = 2201 then nvl(pay_amount_header, 0)
        end as total_payment_header,
        case
            when y.header_code = 2202 then nvl(pay_amount_header, 0)
        end as other_payment_header,
        case
            when y.detail_code = 10 then nvl(pay_amount_detail, 0)
        end as prin_schedule,
        case
            when y.detail_code = 410 then nvl(pay_amount_detail, 0)
        end as principal_payment,
        case
            when y.detail_code = 420 then nvl(pay_amount_detail, 0)
        end as int_payment,
        case
            when y.detail_code = 20 then nvl(pay_amount_detail, 0)
        end as koreksi_int_payment,
        case
            when y.detail_code = 520 then nvl(pay_amount_detail, 0)
        end as provision_fee_int,
        case
            when y.detail_code = 30 then nvl(pay_amount_detail, 0)
        end as late_charge,
        case
            when y.detail_code = 40 then nvl(pay_amount_detail, 0)
        end as judicial_fee,
        case
            when y.detail_code = 100 then nvl(pay_amount_detail, 0)
        end as penalty_fee,
        case
            when y.detail_code = 325 then nvl(pay_amount_detail, 0)
        end as koreksi_int_psak
    from (
            select a.pt_date,
                a.lhnoac as loan_id,
                a.lhnotc as header_code,
                b.lhnotc as detail_code,
                max(a.pay_date) as last_paydate_header,
                max(b.pay_date) as last_paydate_detail,
                nvl(sum(a.pay_amount), 0) as pay_amount_header,
                nvl(sum(b.pay_amount), 0) as pay_amount_detail
            from actual_header_repaid a
                left join actual_detail_repaid b on a.lhnoac = b.lhnoac
                and a.pt_date = b.pt_date
            group by a.pt_date,
                a.lhnoac,
                a.lhnotc,
                b.lhnotc
        ) y
),
pivot_actual_repay as (
    select pt_date,
        loan_id,
        CASE
            WHEN COLLECT_SET(total_payment_header) [0] IS NULL THEN 0
            ELSE COLLECT_SET(total_payment_header) [0]
        END AS total_payment_header,
        CASE
            WHEN COLLECT_SET(other_payment_header) [0] IS NULL THEN 0
            ELSE COLLECT_SET(other_payment_header) [0]
        END AS other_payment_header,
        CASE
            WHEN COLLECT_SET(principal_payment) [0] IS NULL THEN 0
            ELSE COLLECT_SET(principal_payment) [0]
        END AS principal_payment,
        CASE
            WHEN COLLECT_SET(prin_schedule) [0] IS NULL THEN 0
            ELSE COLLECT_SET(prin_schedule) [0]
        END AS prin_schedule,
        CASE
            WHEN COLLECT_SET(int_payment) [0] IS NULL THEN 0
            ELSE COLLECT_SET(int_payment) [0]
        END AS int_payment,
        CASE
            WHEN COLLECT_SET(koreksi_int_payment) [0] IS NULL THEN 0
            ELSE COLLECT_SET(koreksi_int_payment) [0]
        END AS koreksi_int_payment,
        CASE
            WHEN COLLECT_SET(provision_fee_int) [0] IS NULL THEN 0
            ELSE COLLECT_SET(provision_fee_int) [0]
        END AS provision_fee_int,
        CASE
            WHEN COLLECT_SET(late_charge) [0] IS NULL THEN 0
            ELSE COLLECT_SET(late_charge) [0]
        END AS late_charge,
        CASE
            WHEN COLLECT_SET(judicial_fee) [0] IS NULL THEN 0
            ELSE COLLECT_SET(judicial_fee) [0]
        END AS judicial_fee,
        CASE
            WHEN COLLECT_SET(penalty_fee) [0] IS NULL THEN 0
            ELSE COLLECT_SET(penalty_fee) [0]
        END AS penalty_fee,
        CASE
            WHEN COLLECT_SET(koreksi_int_psak) [0] IS NULL THEN 0
            ELSE COLLECT_SET(koreksi_int_psak) [0]
        END AS koreksi_int_psak
    from final_actual_repayment
    group by pt_date,
        loan_id
)
select b.pt_date,
    b.lmcdpl,
    b.lmcdst,
    b.ec_status,
    b.lmnotp,
    b.lmnofp,
    b.lmrtnm,
    b.disb_date,
    b.disb_month,
    nvl(
        from_unixtime(
            unix_timestamp(
                cast(cast(b.lmdtpo as int) as string),
                'yyyyMMdd'
            ),
            'yyyy-MM-dd'
        ),
        ''
    ) as settle_date,
    sum(b.count_loan) count_loan,
    sum(b.cur_balance) cur_balance,
    sum(b.loan_disb) loan_disb,
    sum(b.prin_schedule) prin_schedule,
    sum(b.int_schedule) int_schedule,
    sum(b.amortized_cost) as amortized_cost,
    sum(b.unamortized_cost) as unamortized_cost,
    sum(a.total_payment_header) as total_payment_header,
    sum(a.other_payment_header) as other_payment_header,
    sum(a.principal_payment) as principal_payment,
    sum(a.int_payment) as int_payment,
    sum(a.koreksi_int_payment) as koreksi_int_payment,
    sum(a.koreksi_int_psak) as koreksi_int_psak
from detail_disbursement_amount b
    left join pivot_actual_repay a on a.pt_date = b.pt_date
    and a.loan_id = b.lmnoac
group by b.pt_date,
    b.lmcdpl,
    b.lmcdst,
    b.ec_status,
    b.lmnotp,
    b.lmnofp,
    b.lmrtnm,
    b.disb_date,
    b.disb_month,
    nvl(
        from_unixtime(
            unix_timestamp(
                cast(cast(b.lmdtpo as int) as string),
                'yyyyMMdd'
            ),
            'yyyy-MM-dd'
        ),
        ''
    )