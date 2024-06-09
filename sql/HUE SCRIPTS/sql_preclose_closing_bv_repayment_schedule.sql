select '${date}' as pt_date,
    b.PD_SEGMENT,
    b.PD_LGD,
    b.PRODUCT_CODE,
    b.ACCOUNT_NO,
    b.CLIENT_NO,
    b.stage_max STAGE,
    b.LOAN_MATURITY_DATE,
    b.CUR_BALANCE,
    b.OUTSTANDING_CONTRACTUAL,
    b.TOTAL_INT_ACCRUED,
    b.DAY_PAST_DUE DPD_acct,
    b.max_dpd DPD_Client,
    b.collectability_max COLLECTABILITY,
    b.SECTOR_CODE,
    b.INT_REAL_RATE,
    b.FAC_AMOUNT,
    b.REVOLVING_FLAG,
    b.REVOLVING_FREEZE_STAT,
    b.RESTRUCTURE,
    b.SICR_OVERRIDE,
    b.PAYMENT_DATE,
    b.PRINCIPAL_PAYMENT,
    b.INTEREST_PAYMENT,
    b.UNAMORTIZED_COST_FEE,
    if(b.gca < 0, 0, b.gca) gca,
    b.OVERDUE_PRINCIPAL,
    b.OVERDUE_INTEREST,
    b.PERIOD,
    b.ecl_bucket,
    b.ecl_bucket_max,
    b.IMPORT_SOURCE,
    b.EC_NUMBER,
    if(b.gca < 0, 0, b.gca) ead_drawn,
    case
        when b.REVOLVING_FLAG = 'Y' then b.fac_amount - if(b.gca < 0, 0, b.gca)
        else 0
    end ead_undrawn,
    case
        when b.REVOLVING_FLAG = 'Y' then b.fac_amount
        else if(b.gca < 0, 0, b.gca)
    end ead_lifetime,
    case
        when PERIOD > 12 then 0
        when b.REVOLVING_FLAG = 'Y' then b.fac_amount
        else if(b.gca < 0, 0, b.gca)
    end ead_12m
from (
        select mk.SEGMENT_1 PD_SEGMENT,
            'UNSECURED' PD_LGD,
            mk.product_code PRODUCT_CODE,
            mk.account_no ACCOUNT_NO,
            mk.client_no CLIENT_NO,
            mk.STAGE STAGE,
            mk.loan_maturity_date LOAN_MATURITY_DATE,
            mk.cur_balance CUR_BALANCE,
            mk.outstanding_contractual OUTSTANDING_CONTRACTUAL,
            mk.day_past_due DAY_PAST_DUE,
            mk.collectability COLLECTABILITY,
            mk.sector_code SECTOR_CODE --   ,0 UNDRAWN_BAL
,
            mk.int_real_rate INT_REAL_RATE,
            mk.fac_amount FAC_AMOUNT,
            Case
                When mk.Repayment_type = 'R' Then 'Y'
                Else 'N'
            End REVOLVING_FLAG,
            '' REVOLVING_FREEZE_STAT,
            mk.restructure RESTRUCTURE,
            0 SICR_OVERRIDE,
            from_unixtime(
                UNIX_TIMESTAMP(
                    cast(cast(bl.L7DTDU as bigint) as string),
                    'yyyyMMdd'
                ),
                'yyyy-MM-dd'
            ) PAYMENT_DATE,
            bl.L7AMPR PRINCIPAL_PAYMENT,
            bl.L7AMIN INTEREST_PAYMENT,
            bl.L7AMEI UNAMORTIZED_COST_FEE,
            CASE
                WHEN max.stage_max = '1'
                and mk.restructure <> 'Restruktur' then mk.CUR_BALANCE + if(
                    mk.TOTAL_INT_ACCRUED < 0,
                    0,
                    mk.TOTAL_INT_ACCRUED
                ) + bl_2.L7AMEI - NVL(
                    SUM(bl.L7AMPR) OVER (
                        PARTITION BY mk.account_no
                        ORDER BY bl.L7DTDU ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ),
                    0
                )
                WHEN max.stage_max = '3' THEN mk.CUR_BALANCE
                WHEN max.stage_max = '2'
                or mk.restructure = 'Restruktur' THEN mk.CUR_BALANCE + if(
                    mk.TOTAL_INT_ACCRUED < 0,
                    0,
                    mk.TOTAL_INT_ACCRUED
                )
            END AS gca,
            row_number() over (
                partition by mk.account_no
                order by bl.l7dtdu asc
            ) rn --        , Row_number() over (partition by mk.account_no order by bl.l7dtdu Desc) - 1 PERIOD
,
            Row_number() over (
                partition by mk.account_no
                order by bl.l7dtdu asc
            ) PERIOD,
            case
                when mk.total_int_accrued < 0 then 0
                else mk.total_int_accrued
            end TOTAL_INT_ACCRUED,
            mk.ecl_bucket ecl_bucket,
            max.ecl_bucket_max ecl_bucket_max --    , max.stage_max stage_max
,
            case
                when max.stage_max = '1'
                and mk.restructure = 'Restruktur' then '2'
                else max.stage_max
            end stage_max,
            max.collectability_max,
            max.max_dpd,
            lbl.LBYAIN OVERDUE_PRINCIPAL,
            lbl.LBYAPR OVERDUE_INTEREST,
            'BankVision' IMPORT_SOURCE,
            'EC1' EC_NUMBER
        from (
                select account_no,
                    client_no,
                    STAGE,
                    loan_maturity_date,
                    cur_balance,
                    outstanding_contractual,
                    total_int_accrued,
                    day_past_due,
                    collectability,
                    sector_code,
                    int_real_rate,
                    fac_amount,
                    restructure,
                    product_code,
                    SEGMENT_1,
                    ecl_bucket,
                    repayment_type,
                    unamortized_cost_fee --, ecl_bucket_max
                from dm.rep_fin_reg_bv_master_kredit_ss_d
                where pt_date = '${date}'
                    and ec_number in ('EC1', 'EC2')
                    and loan_maturity_date > '${date}'
                    and product_code not in ('271', '272', '273', '274', 'DL1', 'DL2')
            ) mk
            left join (
                select client_no,
                    max(day_past_due) max_dpd,
                    CASE
                        WHEN max(day_past_due) = 0 THEN 1
                        WHEN max(day_past_due) > 0
                        and max(day_past_due) <= 90 THEN 2
                        WHEN max(day_past_due) > 90
                        and max(day_past_due) <= 120 THEN 3
                        WHEN max(day_past_due) > 120
                        and max(day_past_due) <= 180 THEN 4
                        ELSE 5
                    END COLLECTABILITY_MAX,
                    CASE
                        WHEN max(day_past_due) = 0 THEN 1
                        WHEN max(day_past_due) > 0
                        and max(day_past_due) <= 30 THEN 2
                        WHEN max(day_past_due) > 30
                        and max(day_past_due) <= 60 THEN 3
                        WHEN max(day_past_due) > 60
                        and max(day_past_due) <= 90 THEN 4
                        ELSE 5
                    END ECL_BUCKET_MAX,
                    CASE
                        WHEN max(day_past_due) > 90 THEN 3
                        WHEN (
                            max(day_past_due) > 30
                            AND max(day_past_due) <= 90
                        ) --OR RESTRUCTURE = 'Y'
                        THEN 2
                        ELSE 1
                    END STAGE_MAX
                from dm.rep_fin_reg_bv_master_kredit_ss_d
                where pt_date = '${date}'
                    and loan_maturity_date > '${date}'
                group by client_no --, RESTRUCTURE
            ) max on mk.client_no = max.client_no
            left join (
                select L7NOAC,
                    l7dtdu,
                    L7AMEI,
                    L7AMPR,
                    L7AMIN
                from ods.bke_visiona_lbillx_ss
                where pt_date = '${date}'
            ) bl on mk.ACCOUNT_NO = bl.L7NOAC
            left join (
                select L7NOAC,
                    L7AMEI,
                    L7AMPR,
                    L7DTDU
                from (
                        select L7NOAC,
                            L7AMEI,
                            L7AMPR,
                            L7DTDU,
                            Row_number() over (
                                partition by L7NOAC
                                order by l7dtdu asc
                            ) rn
                        from ods.bke_visiona_lbillx_ss ob1
                        where pt_date = '${date}'
                            and from_unixtime(
                                UNIX_TIMESTAMP(
                                    cast(cast(L7DTDU as bigint) as string),
                                    'yyyyMMdd'
                                ),
                                'yyyy-MM-dd'
                            ) > '${date}'
                    ) temp
                where temp.rn = 1
            ) bl_2 on mk.ACCOUNT_NO = bl_2.L7NOAC
            left join (
                select lbnoac,
                    LBYAIN,
                    LBYAPR,
                    lbdtxd
                from ods.bke_visiona_lbill_ss
                where pt_date = '${date}'
            ) lbl on bl.L7NOAC = lbl.lbnoac
            and bl.L7DTDU = lbl.lbdtxd
        where from_unixtime(
                UNIX_TIMESTAMP(
                    cast(cast(bl.L7DTDU as bigint) as string),
                    'yyyyMMdd'
                ),
                'yyyy-MM-dd'
            ) > '${date}'
    ) b;
----- 
select pt_date,
    pd_segment,
    pd_lgd,
    product_code,
    account_no,
    cast(client_no as bigint),
    stage,
    loan_maturity_date,
    cur_balance,
    outstanding_contractual,
    total_int_accrued,
    dpd_acct,
    dpd_client,
    collectability,
    sector_code,
    int_real_rate,
    fac_amount,
    revolving_flag,
    revolving_freeze_stat,
    restructure,
    sicr_override,
    payment_date,
    principal_payment,
    interest_payment,
    unamortized_cost_fee,
    gca,
    overdue_principal,
    overdue_interest,
    period,
    ecl_bucket,
    ecl_bucket_max,
    import_source,
    ec_number,
    ead_drawn,
    ead_undrawn,
    ead_lifetime,
    ead_12m
from dm.rep_fin_reg_bv_repayment_schedule_ecl_ss_d
where pt_Date = '${date}';