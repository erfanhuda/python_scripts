select case
        when product_code in (101, 102) then 'SPL'
        when product_code in (103, 104) THEN 'BCL'
        when product_code = 105 then 'SCL'
    end as product_type,
    tenor,
    sum(loan_disb) as disb_amount,
    substring(loan_disbursement_date, 1, 7) as disb_month
from dm.rep_fin_reg_db_master_kredit_settled_ss_m
where pt_date = last_day(settle_date)
    and product_code in (101, 102, 103, 104, 105)
    and tenor != 2
    and loan_disbursement_date >= '2021-06-01'
    and pt_date <= '2023-09-30'
group by case
        when product_code in (101, 102) then 'SPL'
        when product_code in (103, 104) THEN 'BCL'
        when product_code = 105 then 'SCL'
    end,
    tenor,
    substring(loan_disbursement_date, 1, 7);