CREATE TABLE id_finpro.write_off_contract_disb as (
    select pt_date,
        case
            when product_code in ('101', '102') then 'SPL'
            WHEN product_code in ('103', '104') then 'BCL'
            WHEN PRODUCT_CODE = '105' THEN 'SCL'
            else product_type
        end as product_type,
        tenor_in_month,
        substring(loan_disbursement_date, 1, 7) as disburse_month,
        substring(written_off_date, 1, 7) as wo_month,
        substring(clawback_date, 1, 7) as clawback_month,
        sum(write_off_principal) as wo_prin,
        sum(clawback_principal) as clawback_prin,
        round(
            months_between(
                last_Day(written_off_date),
                last_day(loan_disbursement_date)
            ),
            0
        ) as mob
    from dm.rep_fin_reg_db_master_kredit_write_off_ss_d
    where pt_date = last_day(pt_date)
        and pt_date between '2022-01-31' and '2023-08-31'
    group by pt_date,
        case
            when product_code in ('101', '102') then 'SPL'
            WHEN product_code in ('103', '104') then 'BCL'
            WHEN PRODUCT_CODE = '105' THEN 'SCL'
            else product_type
        end,
        tenor_in_month,
        substring(loan_disbursement_date, 1, 7),
        substring(written_off_date, 1, 7),
        substring(clawback_date, 1, 7),
        round(
            months_between(
                last_Day(written_off_date),
                last_day(loan_disbursement_date)
            ),
            0
        )
);