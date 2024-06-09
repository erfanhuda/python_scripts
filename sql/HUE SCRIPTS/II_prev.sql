---prev accrued interest---
select add_months(pt_date, 1),
    case
        when product_code in ('101', '102') then 'SPL'
        when product_code in ('103', '104') then 'BCL'
        when product_code = '105' then 'SCL'
        when product_code = '106' then 'KPL'
        when product_code = '107' then 'EML'
        when product_code = '108' then 'RCL'
        when product_code = '109' then 'AKL'
        when product_code = '110' then 'EAL'
        when product_code = '111' then 'PYL'
        when product_code = '112' then 'UDL'
        when product_code = '115' then 'APL'
        when product_code = 'C01' then 'APL'
        when product_code = 'M05' then 'SME'
        when product_code = 'SC1' then 'SCF'
        when product_code = 'SC2' then 'SCF'
    END AS PRODUCT_TYPE,
    a.tenor,
    case
        when a.product_code IN ('108', '109', '110') then nvl(
            a.tenor_in_month,
            round(
                months_between(a.loan_maturity_date, A.loan_disbursement_date),
                0
            )
        )
        else a.tenor
    end as tenor_in_month,
    a.payment_freq,
    substring(loan_disbursement_date, 1, 7) as disburse_month --, b.ecl_bucket_max
,
    int_real_rate,
    sum(total_int_accrued_adj) as total_int_accrued_adj,
    sum(total_int_accrued) as total_int_accrued
from dm.rep_fin_reg_com_master_kredit_ss_d as a
    left join (
        select pt_date,
            client_no,
            ecl_bucket_max
        from dm.rep_fin_reg_com_master_kredit_ss_d
        where pt_Date = last_day(pt_date)
            and product_code in (
                '101',
                '102',
                '103',
                '104',
                '105',
                '106',
                '107',
                '108',
                '109',
                '110',
                '111',
                '112',
                '115',
                'C01',
                'M05',
                'SC1',
                'SC2'
            )
        group by pt_date,
            client_no,
            ecl_bucket_max
    ) as b on a.client_no = b.client_no
    and add_months(a.pt_date, -1) = b.pt_date
where a.pt_Date = last_Day(a.pt_date)
    and a.pt_date = add_months('${date}', -1)
    and a.product_code in (
        '101',
        '102',
        '103',
        '104',
        '105',
        '106',
        '107',
        '108',
        '109',
        '110',
        '111',
        '112',
        '115',
        'C01',
        'M05',
        'SC1',
        'SC2'
    )
    and a.col in ('1', '2')
group by a.pt_date,
    case
        when product_code in ('101', '102') then 'SPL'
        when product_code in ('103', '104') then 'BCL'
        when product_code = '105' then 'SCL'
        when product_code = '106' then 'KPL'
        when product_code = '107' then 'EML'
        when product_code = '108' then 'RCL'
        when product_code = '109' then 'AKL'
        when product_code = '110' then 'EAL'
        when product_code = '111' then 'PYL'
        when product_code = '112' then 'UDL'
        when product_code = '115' then 'APL'
        when product_code = 'C01' then 'APL'
        when product_code = 'M05' then 'SME'
        when product_code = 'SC1' then 'SCF'
        when product_code = 'SC2' then 'SCF'
    END,
    a.tenor,
case
        when a.product_code IN ('108', '109', '110') then nvl(
            a.tenor_in_month,
            round(
                months_between(a.loan_maturity_date, A.loan_disbursement_date),
                0
            )
        )
        else a.tenor
    end,
    a.payment_freq,
    int_real_rate,
    substring(loan_disbursement_date, 1, 7)