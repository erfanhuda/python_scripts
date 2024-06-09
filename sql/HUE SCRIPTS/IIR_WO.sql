with repayment as (
    select a.loan_no,
        sum(
            case
                when a.accounting_status = 'WRITE_OFF' then a.repay_interest
                else 0
            end
        ) as repay_interest_detil_wo,
        sum(
            case
                when a.accounting_status <> 'WRITE_OFF' then a.repay_interest
                else 0
            end
        ) as repay_interest_detil_active
    from dm.rep_fin_reg_loan_repay_detail_ss_d a
        inner join dm.rep_fin_reg_db_master_kredit_write_off_ss_d b on a.loan_no = b.loan_no
    where a.pt_date <= '${date}'
        and b.pt_date = '${date}'
    group by a.loan_no
),
repayment_type as (
    select a.loan_no,
        a.repay_type,
        sum(
            case
                when a.accounting_status = 'WRITE_OFF' then a.repay_interest
                else 0
            end
        ) as repay_interest_detil_wo,
        sum(
            case
                when a.accounting_status <> 'WRITE_OFF' then a.repay_interest
                else 0
            end
        ) as repay_interest_detil_active
    from dm.rep_fin_reg_loan_repay_detail_ss_d a
        inner join dm.rep_fin_reg_db_master_kredit_write_off_ss_d b on a.loan_no = b.loan_no
    where a.pt_date <= '${date}'
        and b.pt_date = '${date}'
    group by a.loan_no,
        a.repay_type
),
wo as (
    select a.pt_date,
        a.loan_no,
        a.product_code,
        a.tenor,
        case
            when product_code in ('108', '109', '110') then nvl(
                tenor_in_month,
                round(
                    months_between(A.loan_maturity_date, A.loan_disbursement_date),
                    0
                )
            )
            else tenor
        end as tenor_in_month,
        a.payment_freq,
        a.int_real_rate,
        a.loan_disbursement_date,
        a.loan_maturity_date,
        a.loan_disb,
        a.write_off_principal,
        a.write_off_interest,
        a.recovery_principal,
        a.recovery_interest,
        a.refund_principal,
        a.refund_interest,
        a.clawback_principal,
        a.clawback_interest,
        sum(b.principal) as principal_schedule,
        sum(b.interest) as interest_schedule,
        sum(b.repaid_principal) as repaid_principal,
        sum(b.repaid_interest) as repaid_interest
    from dm.rep_fin_reg_db_master_kredit_write_off_ss_d a
        left join ods.cbs_id_bke_loan_core_db_repay_plan_tab_ss b on a.pt_date = b.pt_date
        and a.loan_no = b.loan_no
    where a.pt_date = '${date}'
    group by a.pt_date,
        a.loan_no,
        a.product_code,
        a.tenor,
        case
            when product_code in ('108', '109', '110') then nvl(
                tenor_in_month,
                round(
                    months_between(A.loan_maturity_date, A.loan_disbursement_date),
                    0
                )
            )
            else tenor
        end,
        a.payment_freq,
        a.int_real_rate,
        a.loan_disbursement_date,
        a.loan_maturity_date,
        a.loan_disb,
        a.write_off_principal,
        a.write_off_interest,
        a.recovery_principal,
        a.recovery_interest,
        a.refund_principal,
        a.refund_interest,
        a.clawback_principal,
        a.clawback_interest
)
select a.pt_date,
    count(a.loan_no) as count_loan,
    a.product_code,
    a.tenor,
    a.tenor_in_month,
    a.payment_freq,
    a.int_real_rate,
    a.loan_disbursement_date,
    substring(a.loan_maturity_date, 1, 7) as loan_maturity_month,
    sum(a.loan_disb) as loan_disb_amt,
    sum(a.write_off_principal) as wo_prin,
    sum(a.write_off_interest) as wo_int,
    sum(nvl(a.recovery_principal, 0)) as wo_recovery_prin,
    sum(nvl(a.recovery_interest, 0)) as wo_recovery_int,
    sum(nvl(a.refund_principal, 0)) as wo_refund_prin,
    sum(nvl(a.refund_interest, 0)) as wo_refund_int,
    sum(nvl(a.clawback_principal, 0)) as wo_clawback_prin,
    sum(nvl(a.clawback_interest, 0)) as wo_clawback_int,
    sum(nvl(a.principal_schedule, 0)) as principal_schedule,
    sum(nvl(a.interest_schedule, 0)) as interest_schedule,
    sum(nvl(a.repaid_principal, 0)) as repaid_principal,
    sum(
        nvl(b.repay_interest_detil_active, 0) + nvl(b.repay_interest_detil_wo, 0)
    ) as repaid_interest --,sum(nvl(a.repaid_interest,0)) as repaid_interest
,
    sum(
        nvl(c.repay_interest_detil_active, 0) + nvl(c.repay_interest_detil_wo, 0)
    ) as repaid_interest_TF,
    sum(
        nvl(d.repay_interest_detil_active, 0) + nvl(d.repay_interest_detil_wo, 0)
    ) as repaid_interest_NP,
    sum(
        nvl(e.repay_interest_detil_active, 0) + nvl(e.repay_interest_detil_wo, 0)
    ) as repaid_interest_FP,
    sum(
        nvl(f.repay_interest_detil_active, 0) + nvl(f.repay_interest_detil_wo, 0)
    ) as repaid_interest_EP,
    sum(
        nvl(g.repay_interest_detil_active, 0) + nvl(g.repay_interest_detil_wo, 0)
    ) as repaid_interest_CLAWBACK,
    sum(
        nvl(h.repay_interest_detil_active, 0) + nvl(h.repay_interest_detil_wo, 0)
    ) as repaid_interest_BUYBACK,
    sum(b.repay_interest_detil_active) as repay_interest_detil_active,
    sum(b.repay_interest_detil_wo) as repay_interest_detil_wo
from wo a
    left join repayment b on a.loan_no = b.loan_no
    left join (
        select *
        from repayment_type
        where repay_type = 'TF'
    ) c on a.loan_no = c.loan_no
    left join (
        select *
        from repayment_type
        where repay_type = 'NP'
    ) d on a.loan_no = d.loan_no
    left join (
        select *
        from repayment_type
        where repay_type = 'FP'
    ) e on a.loan_no = e.loan_no
    left join (
        select *
        from repayment_type
        where repay_type = 'EP'
    ) f on a.loan_no = f.loan_no
    left join (
        select *
        from repayment_type
        where repay_type = 'CLAWBACK'
    ) g on a.loan_no = g.loan_no
    left join (
        select *
        from repayment_type
        where repay_type = 'BUYBACK'
    ) h on a.loan_no = h.loan_no
group by a.pt_date,
    a.product_code,
    a.tenor,
    a.tenor_in_month,
    a.payment_freq,
    a.int_real_rate,
    a.loan_disbursement_date,
    substring(a.loan_maturity_date, 1, 7)