with mk_com as (
    select pt_date,
        loan_no,
        import_source,
        product_code,
        day_past_due,
        tenor,
        tenor_in_month,
        payment_freq,
        loan_disbursement_date,
        loan_maturity_date,
        int_real_rate,
        loan_disb,
        cur_balance,
        unamortized_cost_fee,
        collectability,
        int_inc_reversal,
        last_day(loan_disbursement_date) as disb_month,
        max(collectability) over (partition by pt_date, client_no) as col_max
    from dm.rep_fin_reg_com_master_kredit_ss_d
    where pt_date = '${date}'
        and import_source = 'Digibank'
),
repayment as (
    select a.loan_no,
        sum(a.repay_principal) as repay_principal,
        sum(a.repay_interest) as repay_interest,
        sum(nvl(b.int_inc_reversal, 0)) - sum(nvl(a.repay_interest, 0)) as int_inc_reversal
    from dm.rep_fin_reg_loan_repay_detail_ss_d a
        inner join mk_com b on a.loan_no = b.loan_no
    where a.pt_date <= '${date}'
        and b.pt_date = '${date}'
        and b.col_max <= 2
    group by a.loan_no
),
repayment_type as (
    select loan_no,
        repay_type,
        sum(a.repay_principal) as repay_principal,
        sum(a.repay_interest) as repay_interest
    from dm.rep_fin_reg_loan_repay_detail_ss_d a
        inner join mk_com b on a.loan_no = b.loan_no
    where a.pt_date <= '${date}'
        and b.pt_date = '${date}'
        and b.col_max <= 2
    group by a.loan_no,
        a.repay_type
),
active as (
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
        a.disb_month,
        substring(a.loan_maturity_date, 1, 7) as loan_maturity_month,
        a.loan_disb,
        a.cur_balance,
        a.unamortized_cost_fee,
        a.int_inc_reversal,
        sum(b.principal) as principal_schedule,
        sum(b.interest) as interest_schedule,
        sum(b.repaid_principal) as repaid_principal,
        sum(b.repaid_interest) as repaid_interest
    from mk_com a
        left join ods.cbs_id_bke_loan_core_db_repay_plan_tab_ss b on a.pt_date = b.pt_date
        and a.loan_no = b.loan_no
    where a.pt_date = '${date}'
        and a.col_max <= 2
        and a.import_source = 'Digibank'
    group by a.pt_date,
        a.loan_no,
        a.product_code,
        a.tenor,
        a.int_inc_reversal,
        a.disb_month,
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
        payment_freq,
        a.int_real_rate,
        a.loan_disbursement_date,
        substring(a.loan_maturity_date, 1, 7),
        a.loan_disb,
        a.cur_balance,
        a.unamortized_cost_fee
)
select a.pt_date,
    count(a.loan_no) as count_loan,
    a.product_code,
    a.tenor,
    a.tenor_in_month,
    a.payment_freq,
    a.int_real_rate,
    a.disb_month,
    a.loan_maturity_month,
    sum(a.loan_disb) as loan_disb_amt,
    sum(a.cur_balance) as cur_balance,
    sum(a.unamortized_cost_fee) as unamortized_cf,
    sum(nvl(a.int_inc_reversal, 0)) as int_inc_reversal,
    sum(nvl(a.principal_schedule, 0)) as principal_schedule,
    sum(nvl(a.interest_schedule, 0)) as interest_schedule,
    sum(nvl(b.repay_principal, 0)) as repay_principal_detail,
    sum(nvl(b.repay_interest, 0)) as repay_interest_detail,
    sum(nvl(c.repay_interest, 0)) as repay_interest_TF,
    sum(nvl(d.repay_interest, 0)) as repay_interest_NP,
    sum(nvl(e.repay_interest, 0)) as repay_interest_FP,
    sum(nvl(f.repay_interest, 0)) as repay_interest_EP,
    sum(nvl(g.repay_interest, 0)) as repay_interest_CLAWBACK,
    sum(nvl(h.repay_interest, 0)) as repay_interest_BUYBACK
from active a
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
    a.disb_month,
    a.loan_maturity_month;