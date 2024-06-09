create table id_finpro.ecl_term_structure_20231130 as
select MASTER_DATA.LOAN_NO LOAN_NO,
    MASTER_DATA.CLIENT_NO CLIENT_NO,
    MASTER_DATA.PRODUCT_CODE PRODUCT_CODE,
    MASTER_DATA.PRODUCT_TYPE PRODUCT_TYPE,
    MASTER_DATA.PD_SEGMENT PD_SEGMENT,
    MASTER_DATA.LGD_SEGMENT LGD_SEGMENT,
    'Collective' COLL_INDIV_FLAG,
    MASTER_DATA.collectability_client COLLECTIVE_CLIENT,
    MASTER_DATA.ECL_STAGE_CLIENT ECL_STAGE_CLIENT,
    MASTER_DATA.ECL_BUCKET_CLIENT ECL_BUCKET_CLIENT,
    MASTER_DATA.DAY_PAST_DUE_ACCT DAY_PAST_DUE_ACCT,
    MASTER_DATA.DAY_PAST_DUE_CLIENT DAY_PAST_DUE_CLIENT,
    MASTER_DATA.SICR_FLAG SICR_FLAG,
    ECL_REPAYMENT_SCHEDULE.repayment_flag EARLY_REPAYMENT_FLAG,
    ECL_REPAYMENT_SCHEDULE.PAYMENT_DATE PAYMENT_DATE,
    MASTER_DATA.LOAN_MATURITY_DATE LOAN_MATURITY_DATE,
    MASTER_DATA.TENOR_REMAINING TENOR_REMAINING,
    CASE
        WHEN ECL_STAGE_CLIENT >= 2 THEN NVL(LIFETIME_OUTPUT_BEHAVIORIAL.LIFETIME_EXCEED, 0) + NVL(LIFETIME_OUTPUT_AFTER.LIFETIME_EXCEED, 0)
        ELSE NVL(LIFETIME_OUTPUT_AFTER.LIFETIME_EXCEED, 0)
    END AS LIFETIME,
    ECL_REPAYMENT_SCHEDULE.PERIOD PERIOD,
    MASTER_DATA.LIMIT_USAGE_RATIO LIMIT_USAGE_RATIO,
    MASTER_DATA.LIMIT_USAGE_BUCKET LIMIT_USAGE_BUCKET,
    MASTER_DATA.LOAN_LIMIT LOAN_LIMIT,
    MASTER_DATA.ACCRUED_INTEREST ACCRUED_INTEREST,
    MASTER_DATA.ACCRUED_INTEREST_PL ACCRUED_INTEREST_PL,
    MASTER_DATA.CUR_BALANCE CUR_BALANCE,
    ECL_REPAYMENT_SCHEDULE.PRINCIPAL_PAYMENT PRINCIPAL,
    ECL_REPAYMENT_SCHEDULE.INTEREST_PAYMENT INTEREST,
    nvl(PD_OUTPUT.PD_BASE, 0) PD_BASE,
    nvl(PD_OUTPUT.PD_BEST, 0) PD_BEST,
    nvl(PD_OUTPUT.PD_WORST, 0) PD_WORST,
    nvl(MASTER_CONFIG.RATIO_BASE, 0) RATIO_BASE,
    nvl(MASTER_CONFIG.RATIO_BEST, 0) RATIO_BEST,
    nvl(MASTER_CONFIG.RATIO_WORST, 0) RATIO_WORST -- 权重 来算 pd
,
    nvl(
        (
            nvl(PD_OUTPUT.PD_BASE, 0) * MASTER_CONFIG.RATIO_BASE + nvl(PD_OUTPUT.PD_BEST, 0) * MASTER_CONFIG.RATIO_BEST + nvl(PD_OUTPUT.PD_WORST, 0) * MASTER_CONFIG.RATIO_WORST
        ),
        0
    ) PD_WEIGHTED,
    nvl(LGD_OUTPUT.LGD, 1) LGD -- 用的
,
    nvl(ECL_REPAYMENT_SCHEDULE.EAD_DRAWN, 0) EAD_DRAWN -- 没有用的
,
    nvl(
        (
            case
                when MASTER_DATA.ECL_BUCKET_CLIENT > 1 then 0
                else ECL_REPAYMENT_SCHEDULE.EAD_UNDRAWN
            end
        ),
        0
    ) EAD_UNDRAWN,
    nvl(CCF_OUTPUT.CCF, 0) CCF,
    nvl(
        case
            when MASTER_DATA.ECL_STAGE_CLIENT = 3 then 1 --       计算 到时候 现在到还款日期 的 折现值
            else 1 / POWER(
                (1 + ECL_REPAYMENT_SCHEDULE.INT_REAL_RATE),
                DATEDIFF(ECL_REPAYMENT_SCHEDULE.PAYMENT_DATE, '${dt}') / 360
            )
        end,
        0
    ) DISCOUNT_RATE,
    nvl(ECL_REPAYMENT_SCHEDULE.INT_REAL_RATE, 0) INT_REAL_RATE,
    nvl(
        ECL_Multiplier_PRIORITY.MULTIPLIER,
        MASTER_CONFIG.MULTIPLIER
    ) MULTIPLIER -- EAD*DISCOUNT_RATE
,
    nvl(ECL_REPAYMENT_SCHEDULE.EAD_DRAWN, 0) * nvl(LGD_OUTPUT.LGD, 1) * nvl(PD_OUTPUT.PD_BASE, 0) * nvl(
        case
            when MASTER_DATA.ECL_STAGE_CLIENT = 3 then 1 --       计算 到时候 现在到还款日期 的 折现值
            else 1 / POWER(
                (1 + ECL_REPAYMENT_SCHEDULE.INT_REAL_RATE),
                DATEDIFF(ECL_REPAYMENT_SCHEDULE.PAYMENT_DATE, '${dt}') / 360
            )
        end,
        0
    ) ECL_DRAWN_BASE,
    nvl(ECL_REPAYMENT_SCHEDULE.EAD_DRAWN, 0) * nvl(LGD_OUTPUT.LGD, 1) * nvl(PD_OUTPUT.PD_BEST, 0) * nvl(
        case
            when MASTER_DATA.ECL_STAGE_CLIENT = 3 then 1 --       计算 到时候 现在到还款日期 的 折现值
            else 1 / POWER(
                (1 + ECL_REPAYMENT_SCHEDULE.INT_REAL_RATE),
                DATEDIFF(ECL_REPAYMENT_SCHEDULE.PAYMENT_DATE, '${dt}') / 360
            )
        end,
        0
    ) ECL_DRAWN_BEST,
    nvl(ECL_REPAYMENT_SCHEDULE.EAD_DRAWN, 0) * nvl(LGD_OUTPUT.LGD, 1) * nvl(PD_OUTPUT.PD_WORST, 0) * nvl(
        case
            when MASTER_DATA.ECL_STAGE_CLIENT = 3 then 1 --       计算 到时候 现在到还款日期 的 折现值
            else 1 / POWER(
                (1 + ECL_REPAYMENT_SCHEDULE.INT_REAL_RATE),
                DATEDIFF(ECL_REPAYMENT_SCHEDULE.PAYMENT_DATE, '${dt}') / 360
            )
        end,
        0
    ) ECL_DRAWN_WORST,
    nvl(
        nvl(ECL_REPAYMENT_SCHEDULE.EAD_DRAWN, 0) * nvl(LGD_OUTPUT.LGD, 1) * nvl(
            ECL_Multiplier_PRIORITY.MULTIPLIER,
            MASTER_CONFIG.MULTIPLIER
        ) * nvl(
            case
                when MASTER_DATA.ECL_STAGE_CLIENT = 3 then 1
                else 1 / POWER(
                    (1 + ECL_REPAYMENT_SCHEDULE.INT_REAL_RATE),
                    DATEDIFF(ECL_REPAYMENT_SCHEDULE.PAYMENT_DATE, '${dt}') / 360
                )
            end,
            0
        ) * nvl(
            (
                nvl(PD_OUTPUT.PD_BASE, 0) * MASTER_CONFIG.RATIO_BASE + nvl(PD_OUTPUT.PD_BEST, 0) * MASTER_CONFIG.RATIO_BEST + nvl(PD_OUTPUT.PD_WORST, 0) * MASTER_CONFIG.RATIO_WORST
            ),
            0
        ),
        0
    ) ECL_DRAWN_WEIGHTED,
    CASE
        WHEN MASTER_DATA.ECL_BUCKET_CLIENT = 1 --EAD 余额+欠息+复息+罚息+计提
        THEN nvl(
            (
                case
                    when MASTER_DATA.ECL_BUCKET_CLIENT > 1 then 0
                    else ECL_REPAYMENT_SCHEDULE.EAD_UNDRAWN
                end
            ),
            0
        ) * nvl(LGD_OUTPUT.LGD, 1) * nvl(PD_OUTPUT.PD_BASE, 0) * nvl(CCF_OUTPUT.CCF, 0) * nvl(
            case
                when MASTER_DATA.ECL_STAGE_CLIENT = 3 then 1 --       计算 到时候 现在到还款日期 的 折现值
                else 1 / POWER(
                    (1 + ECL_REPAYMENT_SCHEDULE.INT_REAL_RATE),
                    DATEDIFF(ECL_REPAYMENT_SCHEDULE.PAYMENT_DATE, '${dt}') / 360
                )
            end,
            0
        )
        ELSE 0
    END ECL_UNDRAWN_BASE,
    CASE
        WHEN MASTER_DATA.ECL_BUCKET_CLIENT = 1 -- ccf 风险转化系数
        THEN nvl(
            (
                case
                    when MASTER_DATA.ECL_BUCKET_CLIENT > 1 then 0
                    else ECL_REPAYMENT_SCHEDULE.EAD_UNDRAWN
                end
            ),
            0
        ) * nvl(LGD_OUTPUT.LGD, 1) * nvl(PD_OUTPUT.PD_BEST, 0) * nvl(CCF_OUTPUT.CCF, 0) * nvl(
            case
                when MASTER_DATA.ECL_STAGE_CLIENT = 3 then 1 --       计算 到时候 现在到还款日期 的 折现值
                else 1 / POWER(
                    (1 + ECL_REPAYMENT_SCHEDULE.INT_REAL_RATE),
                    DATEDIFF(ECL_REPAYMENT_SCHEDULE.PAYMENT_DATE, '${dt}') / 360
                )
            end,
            0
        )
        ELSE 0
    END ECL_UNDRAWN_BEST,
    CASE
        WHEN MASTER_DATA.ECL_BUCKET_CLIENT = 1 THEN nvl(
            (
                case
                    when MASTER_DATA.ECL_BUCKET_CLIENT > 1 then 0
                    else ECL_REPAYMENT_SCHEDULE.EAD_UNDRAWN
                end
            ),
            0
        ) * nvl(LGD_OUTPUT.LGD, 1) * nvl(PD_OUTPUT.PD_WORST, 0) * nvl(CCF_OUTPUT.CCF, 0) * nvl(
            case
                when MASTER_DATA.ECL_STAGE_CLIENT = 3 then 1 --       计算 到时候 现在到还款日期 的 折现值
                else 1 / POWER(
                    (1 + ECL_REPAYMENT_SCHEDULE.INT_REAL_RATE),
                    DATEDIFF(ECL_REPAYMENT_SCHEDULE.PAYMENT_DATE, '${dt}') / 360
                )
            end,
            0
        )
        ELSE 0
    END ECL_UNDRAWN_WORST,
    CASE
        WHEN MASTER_DATA.ECL_BUCKET_CLIENT = 1 THEN nvl(
            (
                case
                    when MASTER_DATA.ECL_BUCKET_CLIENT > 1 then 0
                    else ECL_REPAYMENT_SCHEDULE.EAD_UNDRAWN
                end
            ),
            0
        ) * nvl(LGD_OUTPUT.LGD, 1) * nvl(
            ECL_Multiplier_PRIORITY.MULTIPLIER,
            MASTER_CONFIG.MULTIPLIER
        ) * nvl(CCF_OUTPUT.CCF, 0) * nvl(
            case
                when MASTER_DATA.ECL_STAGE_CLIENT = 3 then 1
                else 1 / POWER(
                    (1 + ECL_REPAYMENT_SCHEDULE.INT_REAL_RATE),
                    DATEDIFF(ECL_REPAYMENT_SCHEDULE.PAYMENT_DATE, '${dt}') / 360
                )
            end,
            0
        ) * nvl(
            (
                nvl(PD_OUTPUT.PD_BASE, 0) * MASTER_CONFIG.RATIO_BASE + nvl(PD_OUTPUT.PD_BEST, 0) * MASTER_CONFIG.RATIO_BEST + nvl(PD_OUTPUT.PD_WORST, 0) * MASTER_CONFIG.RATIO_WORST
            ),
            0
        )
        ELSE 0
    END ECL_UNDRAWN_WEIGHTED,
    nvl(
        (
            nvl(ECL_REPAYMENT_SCHEDULE.EAD_DRAWN, 0) + (
                case
                    when MASTER_DATA.ECL_BUCKET_CLIENT = 1 then nvl(
                        (
                            case
                                when MASTER_DATA.ECL_BUCKET_CLIENT > 1 then 0
                                else ECL_REPAYMENT_SCHEDULE.EAD_UNDRAWN
                            end
                        ),
                        0
                    ) * nvl(CCF_OUTPUT.CCF, 0)
                    else 0
                end
            )
        ) * nvl(LGD_OUTPUT.LGD, 1) * nvl(
            ECL_Multiplier_PRIORITY.MULTIPLIER,
            MASTER_CONFIG.MULTIPLIER
        ) * nvl(
            case
                when MASTER_DATA.ECL_STAGE_CLIENT = 3 then 1 --       计算 到时候 现在到还款日期 的 折现值
                else 1 / POWER(
                    (1 + ECL_REPAYMENT_SCHEDULE.INT_REAL_RATE),
                    DATEDIFF(ECL_REPAYMENT_SCHEDULE.PAYMENT_DATE, '${dt}') / 360
                )
            end,
            0
        ) * nvl(
            (
                nvl(PD_OUTPUT.PD_BASE, 0) * MASTER_CONFIG.RATIO_BASE + nvl(PD_OUTPUT.PD_BEST, 0) * MASTER_CONFIG.RATIO_BEST + nvl(PD_OUTPUT.PD_WORST, 0) * MASTER_CONFIG.RATIO_WORST
            ),
            0
        ),
        0
    ) ECL_TERM_FINAL,
    MASTER_DATA.tenor
from (
        select LOAN_NO,
            CLIENT_NO,
            PRODUCT_CODE,
            PRODUCT_TYPE,
            PD_SEGMENT,
            LGD_SEGMENT,
            collectability_client,
            ECL_BUCKET_CLIENT,
            LOAN_MATURITY_DATE,
            LIMIT_USAGE_RATIO,
            LIMIT_USAGE_BUCKET,
            LOAN_LIMIT,
            ACCRUED_INTEREST -- non performing > 90
            -- perform < 90
,
            ACCRUED_INTEREST_PL,
            CUR_BALANCE,
            ECL_STAGE_CLIENT,
            DAY_PAST_DUE_CLIENT,
            DAY_PAST_DUE_ACCT,
            SICR_FLAG,
            TENOR_REMAINING,
            tenor
        from ecl.rep_fin_ecl_prep_master_data_ss_m_merge
        where '${dt}' IN (LAST_DAY('${dt}'), DATE_SUB(LAST_DAY('${dt}'), 2))
            and PT_DATE = '${dt}'
            AND SETTLE_TYPE = 'LOAN_OUTSTANDING'
    ) MASTER_DATA
    left join (
        Select LOAN_NO,
            PAYMENT_DATE,
            PERIOD,
            PRINCIPAL_PAYMENT,
            INTEREST_PAYMENT,
            nvl(EAD_DRAWN, 0) as EAD_DRAWN,
            nvl(EAD_UNDRAWN, 0) as EAD_UNDRAWN,
            nvl(INT_REAL_RATE, 0) / 100 as INT_REAL_RATE,
            repayment_flag -- CASE
            -- WHEN CIF_Collectability_Bucket.ECL_BUCKET_MAX = 5 OR CIF_Collectability_Bucket.COL >2
            -- THEN  '3'
            -- WHEN CIF_Collectability_Bucket.ECL_BUCKET_MAX >2 OR NVL(tt.RESTRUCTURE ,"Non Restructure")  = 'YES'
            -- THEN  '2'
            -- ELSE '1'
            -- END                                                   as   STAGE
            --  stage > 90 3个月没有还 高风险
        from ecl.rep_fin_ecl_repayment_schedule_ss_m
        where '${dt}' IN (LAST_DAY('${dt}'), DATE_SUB(LAST_DAY('${dt}'), 2))
            and PT_DATE = '${dt}'
    ) ECL_REPAYMENT_SCHEDULE on MASTER_DATA.LOAN_NO = ECL_REPAYMENT_SCHEDULE.LOAN_NO
    left join (
        Select PD_SEGMENT,
            LIFETIME_EXCEED
        from ecl.rep_fin_ecl_lifetime_param_output_ss_m_merge
        where PT_DATE = last_day(add_months('${dt}', -1))
            AND LIFETIME_TYPE = 'AFTER_MATURITY'
    ) LIFETIME_OUTPUT_AFTER on MASTER_DATA.PD_SEGMENT = LIFETIME_OUTPUT_AFTER.PD_SEGMENT
    left join (
        Select PD_SEGMENT,
            LIFETIME_EXCEED,
            TENOR
        from ecl.rep_fin_ecl_lifetime_param_output_ss_m_merge
        where PT_DATE = last_day(add_months('${dt}', -1))
            AND LIFETIME_TYPE = 'BEHAVIORIAL'
    ) LIFETIME_OUTPUT_BEHAVIORIAL on MASTER_DATA.PD_SEGMENT = LIFETIME_OUTPUT_BEHAVIORIAL.PD_SEGMENT
    AND MASTER_DATA.TENOR = LIFETIME_OUTPUT_BEHAVIORIAL.TENOR
    LEFT JOIN (
        Select PD_SEGMENT,
            LGD_SEGMENT,
            nvl(LGD, 1) as LGD
        from ecl.rep_fin_ecl_lgd_param_output_ss_m_merge
        where PT_DATE = last_day(add_months('${dt}', -1))
    ) LGD_OUTPUT on MASTER_DATA.PD_SEGMENT = LGD_OUTPUT.PD_SEGMENT
    and MASTER_DATA.LGD_SEGMENT = LGD_OUTPUT.LGD_SEGMENT
    left join (
        Select PD_SEGMENT,
            LIMIT_USAGE_RATIO_BUCKET,
            nvl(CCF, 0) as CCF
        from ecl.rep_fin_ecl_ccf_param_output_ss_m_merge
        where PT_DATE = last_day(add_months('${dt}', -1))
    ) CCF_OUTPUT on MASTER_DATA.PD_SEGMENT = CCF_OUTPUT.PD_SEGMENT
    and MASTER_DATA.LIMIT_USAGE_BUCKET = CCF_OUTPUT.LIMIT_USAGE_RATIO_BUCKET
    LEFT JOIN (
        Select PD_SEGMENT,
            PERIOD,
            ecl_bucket_client,
            tenor,
            sum(
                case
                    when scenario = 'BASE' then nvl(PD_BASE, 0)
                    else 0
                end
            ) as PD_BASE,
            sum(
                case
                    when scenario = 'BEST' then nvl(PD_BEST, 0)
                    else 0
                end
            ) as PD_BEST,
            sum(
                case
                    when scenario = 'WORST' then nvl(PD_WORST, 0)
                    else 0
                end
            ) as PD_WORST
        from ecl.rep_fin_ecl_pd_param_output_ss_m_merge
        where PT_DATE = last_day(add_months('${dt}', -1))
        group by PD_SEGMENT,
            PERIOD,
            ECL_BUCKET_CLIENT,
            TENOR
    ) PD_OUTPUT on MASTER_DATA.PD_SEGMENT = PD_OUTPUT.PD_SEGMENT
    and ECL_REPAYMENT_SCHEDULE.PERIOD = PD_OUTPUT.PERIOD
    and MASTER_DATA.ECL_BUCKET_CLIENT = PD_OUTPUT.ecl_bucket_client
    and MASTER_DATA.tenor = PD_OUTPUT.tenor
    cross join (
        SELECT sum(master_config.group_map ['RATIO_BASE']) as RATIO_BASE,
            sum(master_config.group_map ['RATIO_BEST']) as RATIO_BEST,
            sum(master_config.group_map ['RATIO_WORST']) as RATIO_WORST,
            sum(master_config.group_map ['MULTIPLIER']) as MULTIPLIER
        from (
                SELECT map(function_key, value) group_map
                FROM dim.map_fin_ecl_master_config
                WHERE START_DATE <= '${dt}'
                    AND END_DATE >= '${dt}'
            ) MASTER_CONFIG
    ) MASTER_CONFIG on 1 = 1
    left join (
        Select PRODUCT_TYPE,
            TENOR,
            PD_SEGMENT,
            ECL_BUCKET_CLIENT,
            MULTIPLIER
        from dim.map_fin_ecl_multiplier
        WHERE START_DATE <= '${dt}'
            AND END_DATE >= '${dt}'
    ) ECL_Multiplier_PRIORITY on MASTER_DATA.PRODUCT_TYPE = ECL_Multiplier_PRIORITY.PRODUCT_TYPE
    and MASTER_DATA.TENOR = ECL_Multiplier_PRIORITY.TENOR
    and MASTER_DATA.PD_SEGMENT = ECL_Multiplier_PRIORITY.PD_SEGMENT
    and MASTER_DATA.ECL_BUCKET_CLIENT = ECL_Multiplier_PRIORITY.ECL_BUCKET_CLIENT