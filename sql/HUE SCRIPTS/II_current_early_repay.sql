select a.pt_date,
    case
        when a.product_code in ('101', '102') then 'SPL'
        when a.product_code in ('103', '104') then 'BCL'
        when a.product_code = '105' then 'SCL'
        when a.product_code = '106' then 'KPL'
        when a.product_code = '107' then 'EML'
        when a.product_code = '108' then 'RCL'
        when a.product_code = '109' then 'AKL'
        when a.product_code = '110' then 'EAL'
        when a.product_code = '111' then 'PYL'
        when a.product_code = '112' then 'UDL'
        when a.product_code = '115' then 'APL'
        when a.product_code = 'C01' then 'APL'
        when a.product_code = 'M05' then 'SME'
        when a.product_code = 'SC1' then 'SCF'
        when a.product_code = 'SC2' then 'SCF'
    END AS PRODUCT_TYPE,
    a.tenor,
    a.tenor_in_month,
    a.payment_freq,
    a.interest_rate as int_rate,
    substring(b.loan_disbursement_date, 1, 7) as disburse_month,
    sum(a.interest_realised) as sum_interest_realised,
    sum(a.interest_unrealised) as sum_interest_unrealised
from (
        SELECT pt_date,
            repayment_date,
            loan_no,
            product_code,
            interest_realised,
            interest_unrealised,
            tenor,
            tenor_in_month,
            payment_freq,
            interest_rate
        FROM dm.rep_fin_reg_early_repayment_ss_d
        where pt_date = '${end_date_m0}'
            and repayment_date <> "${first_date_m1}"
    ) a
    inner join (
        select loan_no,
            loan_disbursement_date
        from dm.rep_fin_reg_db_master_kredit_ss_d
        where pt_date = '${end_date_m0}'
    ) b on a.loan_no = b.loan_no
group by a.pt_date,
    case
        when a.product_code in ('101', '102') then 'SPL'
        when a.product_code in ('103', '104') then 'BCL'
        when a.product_code = '105' then 'SCL'
        when a.product_code = '106' then 'KPL'
        when a.product_code = '107' then 'EML'
        when a.product_code = '108' then 'RCL'
        when a.product_code = '109' then 'AKL'
        when a.product_code = '110' then 'EAL'
        when a.product_code = '111' then 'PYL'
        when a.product_code = '112' then 'UDL'
        when a.product_code = '115' then 'APL'
        when a.product_code = 'C01' then 'APL'
        when a.product_code = 'M05' then 'SME'
        when a.product_code = 'SC1' then 'SCF'
        when a.product_code = 'SC2' then 'SCF'
    END,
    a.tenor,
    a.tenor_in_month,
    a.payment_freq,
    a.interest_rate,
    substring(b.loan_disbursement_date, 1, 7)
order by a.pt_date;