select a.pt_date,
    a.product_code,
    case
        when a.product_code in ('108', '109', '110') then nvl(
            a.tenor_in_month,
            round(
                months_between(A.loan_maturity_date, A.loan_disbursement_date),
                0
            )
        )
        else tenor
    end as tenor,
    date_format(a.written_off_date, "YYYY-MM") as wo_month,
    date_format(a.clawback_date, "YYYY-MM") as clawback_month,
    a.wo_principal,
    nvl(a.claw_principal, 0) as claw_principal
from (
        select pt_date,
            product_code,
            written_off_date,
            tenor,
            tenor_in_month,
            loan_disbursement_date,
            loan_maturity_date,
            clawback_date,
            sum(write_off_principal) as wo_principal,
            sum(clawback_principal) as claw_principal
        from dm.rep_fin_reg_db_master_kredit_write_off_ss_d
        where pt_date = "${curr_date}"
        group by pt_date,
            product_code,
            written_off_date,
            clawback_date,
            tenor,
            tenor_in_month,
            loan_disbursement_date,
            loan_maturity_date
    ) a