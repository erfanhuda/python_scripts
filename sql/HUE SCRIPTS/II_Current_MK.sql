WITH mk_com as (
    select pt_date,
        product_code,
        tenor,
        tenor_in_month,
        payment_freq,
        int_real_rate,
        loan_disbursement_date,
        total_int_accrued,
        total_int_accrued_adj,
        day_past_due,
        max(day_past_due) over (partition by pt_date, client_no) as day_past_due_client
    from dm.rep_fin_reg_com_master_kredit_ss_d
    where pt_date = '${date}'
        and import_source = 'Digibank'
),
mk_bv as (
    select pt_date,
        lmcdpl as product_code,
        lmnonp as tenor,
        lmnotp as tenor_in_month,
        lmnofp as payment_freq,
        lmrtnm as int_real_rate,
        lmdtnt as loan_disbursement_date,
        lmamai as total_int_accrued,
        0 as total_int_accrued_adj,
        lmnopd as day_past_due,
        max(lmnopd) over (partition by pt_date, lmnoac) as day_past_due_client
    from ods.bke_visiona_lmbal_ss
    where pt_date = '${date}'
        and lmcdpl in ('C01', 'SC1', 'SC2', 'M05')
)
select pt_date,
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
    END AS PRODUCT_TYPE,
    tenor,
    case
        when product_code IN ('108', '109', '110') then tenor_in_month
        else tenor
    end as tenor_in_month,
    payment_freq,
    int_real_rate,
    --ecl_bucket_max as ecl_bucket_client,
    substring(loan_disbursement_date, 1, 7) as disburse_month,
    sum(total_int_accrued) as total_int_accrued,
    sum(total_int_accrued_adj) as total_int_accrued_adj
from mk_com
where product_code in (
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
        '115'
    )
    and pt_Date = last_day(pt_date)
    and pt_Date IN ('${date}')
    and day_past_due_client <= 90
group by pt_date,
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
    END,
    tenor,
    case
        when product_code IN ('108', '109', '110') then tenor_in_month
        else tenor
    end,
    payment_freq,
    int_real_rate,
    substring(loan_disbursement_date, 1, 7)
UNION ALL
select pt_date,
    case
        when product_code in ('SC1', 'SC2') then 'SCF'
        when product_code in ('C01') then 'APL'
        when product_code = 'M05' then 'SME'
    END AS PRODUCT_TYPE,
    tenor,
    tenor_in_month,
    payment_freq,
    round(int_real_rate * 100, 4) as int_real_rate,
    substring(cast(loan_disbursement_date as integer), 1, 6) as disburse_month,
    sum(total_int_accrued) as total_int_accrued,
    sum(total_int_accrued_adj) as total_int_accrued_adj
from mk_bv
where pt_Date = last_day(pt_date)
    and pt_Date IN ('${date}')
    and day_past_due_client <= 90
group by pt_date,
    case
        when product_code in ('SC1', 'SC2') then 'SCF'
        when product_code in ('C01') then 'APL'
        when product_code = 'M05' then 'SME'
    END,
    tenor,
    tenor_in_month,
    payment_freq,
    round(int_real_rate * 100, 4),
    substring(cast(loan_disbursement_date as integer), 1, 6);