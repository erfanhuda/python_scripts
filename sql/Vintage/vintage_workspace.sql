with com_mk as (
    select *, last_day(loan_disbursement_date) as loan_disbursement_month , 'ACTIVE' as source from dm.rep_fin_reg_com_master_kredit_ss_d where pt_date = last_day(pt_date) 
    and product_code in ('101', '102', '103', '104', '105', '106', '107', '108', '109', '110', '111', '112', '115', 'C01', 'SC1', 'SC2', 'M05')
)
, com_wo as (
    select *, last_day(loan_disbursement_date) as loan_disbursement_month , 'WRITE_OFF' as source 
    from dm.rep_fin_reg_db_master_kredit_write_off_ss_d
    where pt_date = date_add(current_date, -2) 
    and product_code in ('101', '102', '103', '104', '105', '106', '107', '108', '109', '110', '111', '112', '115', 'C01', 'SC1', 'SC2', 'M05')
)
, vintage_pop as (
    select A.*, B.* from com_mk A, com_mk B
    where A.loan_disbursement_month <= B.pt_date 
    and A.loan_no = B.loan_no
)