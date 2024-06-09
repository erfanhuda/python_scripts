select pt_date,
    tenor,
    day_past_due,
    max_dpd,
    ecl_bucket,
    max_ecl as max_ecl_cif_level,
    collectability,
    max_col,
    product_code,
    loan_disbursement_date,
    loan_maturity_date,
    count(1) as loan_no,
    sum(loan_disb) as loan_disb,
    sum(cur_balance) as cur_balance,
    sum(total_int_accrued) as total_int_accrued,
    sum(total_int_accrued_adj) as total_int_accrued_adj,
    sum(fac_amount) as fac_amount,
    a.int_real_rate,
    a.tenor_in_month
from (
        select *
        from dm.rep_fin_reg_com_master_kredit_ss_d
        where pt_date = "${date}"
            and (
                import_source = 'Digibank'
                or product_code in('C01', 'SC1', 'M05', 'C02', 'SC2')
            )
    ) as a
    join (
        select b.client_no,
            max (cast (b.ecl_bucket as numeric)) as max_ecl,
            max (cast (b.day_past_due as numeric)) as max_dpd,
            max (cast (b.collectability as numeric)) as max_col
        from dm.rep_fin_reg_com_master_kredit_ss_d as b
        where pt_date = "${date}"
        group by client_no
    ) as c on (a.client_no = c.client_no)
group by pt_date,
    tenor,
    ecl_bucket,
    max_ecl,
    max_col,
    product_code,
    day_past_due,
    collectability,
    loan_disbursement_date,
    loan_maturity_date,
    max_dpd,
    a.int_real_rate,
    a.tenor_in_month;