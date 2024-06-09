select substring (written_off_date, 1, 7) as wo_month,
    case
        when product_code in (101, 102) then 'SPL'
        when product_code in (103, 104) THEN 'BCL'
        when product_code = 105 then 'SCL'
    end as product_type,
    tenor,
    substring (loan_disbursement_date, 1, 7) as disburse_month,
    sum(write_off_principal) as write_off_principal,
    sum(clawback_principal) as clawback_principal
from dm.rep_fin_reg_db_master_kredit_write_off_ss_d
where pt_date = "${end_date}"
    and written_off_date between "${start_date}" and "${end_date}"
    and clawback_date between "${start_date}" and "${end_date}"
group by pt_date,
    case
        when product_code in (101, 102) then 'SPL'
        when product_code in (103, 104) THEN 'BCL'
        when product_code = 105 then 'SCL'
    END,
    tenor,
    substring (loan_disbursement_date, 1, 7),
    substring (written_off_date, 1, 7);