select pt_date,
    case
        when import_source = 'BankVision_LN' then tenor
        else nvl(tenor_in_month, tenor)
    end as tenor_in_month,
    case
        when import_source = 'BankVision_LN' then nvl(payment_freq, 1)
        else payment_freq
    end as payment_freq,
    ecl_bucket,
    ecl_bucket_max,
    col,
    product_code,
    substring (loan_disbursement_date, 1, 7) as disburse_month,
    count(loan_no) as count_loan,
    sum(cur_balance) as cur_balance,
    sum(
        case
            when total_int_accrued < 0 then 0
            else total_int_accrued
        end
    ) as total_int_accrued,
    sum(
        case
            when total_int_accrued_adj < 0 then 0
            else total_int_accrued_adj
        end
    ) as total_int_accrued_adj,
    sum(fac_amount) as fac_amount,
    int_real_rate
from dm.rep_fin_reg_com_master_kredit_ss_d
where pt_date = "${date}"
    and (
        import_source = 'Digibank'
        or product_code in('C01', 'C02', 'M05', 'SC1', 'SC2')
    )
group by pt_date,
    case
        when import_source = 'BankVision_LN' then tenor
        else nvl(tenor_in_month, tenor)
    end,
    case
        when import_source = 'BankVision_LN' then nvl(payment_freq, 1)
        else payment_freq
    end,
    ecl_bucket,
    ecl_bucket_max,
    col,
    product_code,
    substring (loan_disbursement_date, 1, 7),
    int_real_rate