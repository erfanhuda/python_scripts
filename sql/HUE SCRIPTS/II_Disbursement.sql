/*
 - Disbursement
 - Accrued Interest
 - Previous Accrued Interest
 - Early Repay Interest
 - Previous Early Repay Interest
 - Repayment
 - IIR Active NPL
 - IIR Active PL
 - IIR Settled 
 - IIR WO
 */
-- filter date in bv to be like:
-- substring(cast(lmdtnt as integer),1,6) = substring(date_format("${end_date}","YYYYMMDD"),1,6)
-- condition for channeling between -10 first date and -1 of end date
-- condition for jf & BV using normal pt date first date and end date
with disburse_check as (
    select loan_no,
        is_reversal,
        min(pt_date) as min_date
    from dm.rep_fin_reg_loan_disburse_flow_ss_d -- where pt_date between date_add('${start_date}',-10) and date_add('${end_date}',-1)
    where pt_date between '${start_date}' and '${end_date}'
        and disburse_status <> 'LOAN_FAIL'
        and is_reversal = 'N'
    group by loan_no,
        is_reversal
),
disburse_check_channeling as (
    select loan_no,
        is_reversal,
        min(pt_date) as min_date
    from dm.rep_fin_reg_loan_disburse_flow_ss_d
    where pt_date between date_add('${start_date}', -10) and date_add('${end_date}', -1) -- where pt_date between '${start_date}' and '${end_date}'
        and disburse_status <> 'LOAN_FAIL'
        and is_reversal = 'N'
    group by loan_no,
        is_reversal
),
jf_check as (
    select substring(a.pt_date, 1, 7) as disb_month,
        tenor,
        case
            when prod_type in ('108', '109', '110') then nvl(
                tenor_in_month,
                round(
                    months_between(A.loan_maturity_date, A.pt_date),
                    0
                )
            )
            else tenor
        end as tenor_in_month,
        payment_freq,
        prod_type,
        int_real_rate,
        count(a.loan_no) as loan_no,
        SUM(
            case
                when a.is_reversal = 'N' THEN principal
                ELSE principal * -1
            END
        ) as principal
    from dm.rep_fin_reg_loan_disburse_flow_ss_d a
        left join disburse_check b on a.loan_no = b.loan_no
        and a.is_reversal = b.is_reversal
        and a.pt_date > min_date
    where pt_date between '${start_date}' and '${end_date}'
        and disburse_status <> 'LOAN_FAIL'
        and prod_type IN ('101', '102', '103', '104', '105')
        and b.loan_no is null
    group by substring(a.pt_date, 1, 7),
        int_real_rate,
        a.is_reversal,
        tenor,
        case
            when prod_type in ('108', '109', '110') then nvl(
                tenor_in_month,
                round(
                    months_between(A.loan_maturity_date, A.pt_date),
                    0
                )
            )
            else tenor
        end,
        payment_freq,
        prod_type
),
ch_check as (
    select substring(a.pt_date, 1, 7) as disb_month,
        tenor,
        case
            when prod_type in ('108', '109', '110') then nvl(
                tenor_in_month,
                round(
                    months_between(A.loan_maturity_date, A.pt_date),
                    0
                )
            )
            else tenor
        end as tenor_in_month,
        payment_freq,
        prod_type,
        int_real_rate,
        count(a.loan_no) as loan_no,
        SUM(
            case
                when a.is_reversal = 'N' THEN principal
                ELSE principal * -1
            END
        ) as principal
    from dm.rep_fin_reg_loan_disburse_flow_ss_d a
        left join disburse_check_channeling b on a.loan_no = b.loan_no
        and a.is_reversal = b.is_reversal
        and a.pt_date > min_date
    where pt_date between '${start_date}' and '${end_date}'
        and disburse_status <> 'LOAN_FAIL'
        and prod_type IN (
            '106',
            '107',
            '108',
            '109',
            '110',
            '111',
            '112',
            '115'
        )
        and b.loan_no is null
    group by substring(a.pt_date, 1, 7),
        int_real_rate,
        a.is_reversal,
        tenor,
        case
            when prod_type in ('108', '109', '110') then nvl(
                tenor_in_month,
                round(
                    months_between(A.loan_maturity_date, A.pt_date),
                    0
                )
            )
            else tenor
        end,
        payment_freq,
        prod_type
),
accounts as (
    select pt_date,
        nvl(
            last_day(
                from_unixtime(
                    unix_timestamp(cast(cast(lmdtnt as int) as string), 'yyyyMMdd'),
                    'yyyy-MM-dd'
                )
            ),
            ''
        ) as disb_month,
        case
            when lmcdst = 1 then nvl(
                last_day(
                    from_unixtime(
                        unix_timestamp(cast(cast(pt_date as int) as string), 'yyyyMMdd'),
                        'yyyy-MM-dd'
                    )
                ),
                ''
            )
            when lmcdst = 2 then nvl(
                last_day(
                    from_unixtime(
                        unix_timestamp(cast(cast(lmdtpo as int) as string), 'yyyyMMdd'),
                        'yyyy-MM-dd'
                    )
                ),
                ''
            )
        end as ec_month,
        lmnoac,
        lmcdpl,
        lmnotp,
        lmnofp,
        lmrtnm,
        lmnonp,
        lmcdst,
        case
            when lmcdst = 1
            and lmnopd <= 90 then 'Active'
            when lmcdst = 1
            and lmnopd > 90 then 'NPL'
            when lmcdst = 2 then 'Settled'
            when lmcdst = 3 then 'Write Off'
        end as ec_status,
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
        lmnonp,
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
            when lmcdst = 1 then nvl(
                last_day(
                    from_unixtime(
                        unix_timestamp(cast(cast(pt_date as int) as string), 'yyyyMMdd'),
                        'yyyy-MM-dd'
                    )
                ),
                ''
            )
            when lmcdst = 2 then nvl(
                last_day(
                    from_unixtime(
                        unix_timestamp(cast(cast(lmdtpo as int) as string), 'yyyyMMdd'),
                        'yyyy-MM-dd'
                    )
                ),
                ''
            )
        end,
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
        a.disb_month,
        a.lmnoac,
        a.lmcdpl,
        a.lmcdst,
        a.ec_status,
        a.lmnotp,
        a.lmnofp,
        a.lmrtnm,
        a.lmnonp,
        count(1) as count_loan,
        sum(a.cur_balance) as cur_balance,
        sum(a.loan_disb) as loan_disb,
        sum(b.principal_schedule) as prin_schedule,
        sum(b.int_schedule) as int_schedule
    from accounts a
        left join pay_schedule b on a.pt_date = b.pt_date
        and a.lmnoac = b.l7noac
    where a.disb_month = a.pt_date
    group by a.pt_date,
        a.disb_month,
        a.lmnoac,
        a.lmcdpl,
        a.lmcdst,
        a.ec_status,
        a.lmnotp,
        a.lmnofp,
        a.lmrtnm,
        a.lmnonp
),
bv_check as (
    select substring(disb_month, 1, 7) as disb_month,
        lmnotp as tenor,
        lmnonp as tenor_in_month,
        lmnofp as payment_freq,
        lmcdpl as product_code,
        lmrtnm as int_real_rate,
        count(count_loan) as count_loan,
        sum(prin_schedule) as prin_schedule
    from detail_disbursement_amount
    where substring(disb_month, 1, 7) = substring("${end_date}", 1, 7)
    group by substring(disb_month, 1, 7),
        lmnotp,
        lmnonp,
        lmnofp,
        lmcdpl,
        lmrtnm
) -- select * from jf_check
-- UNION ALL
-- select * from ch_check
-- UNION ALL
select *
from bv_check;