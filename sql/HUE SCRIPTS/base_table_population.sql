/* 
 Requirement:
 1. Find population of the months the point date of each account disbursement date OK
 2. Find population of the months the point date of each account settlement date OK
 3. Find population of the months the point date of each account write off date
 4. Find population of the months the point date of each account payment date 
 5. Find population of the months the point date of each account end of month disbursement date OK
 6. Find population of the months the point date of each account end of month settlement date OK
 7. Find population of the months the point date of each account end of month write off date
 8. Find population of the months the point date of each account end of month payment date
 */
with bv_mk as (
    select *,
        nvl(
            from_unixtime(
                unix_timestamp(cast(cast(lmdtnt as int) as string), 'yyyyMMdd'),
                'yyyy-MM-dd'
            ),
            ''
        ) as disb_date,
        case
            when lmcdst = 1 then nvl(
                from_unixtime(
                    unix_timestamp(cast(cast(pt_date as int) as string), 'yyyyMMdd'),
                    'yyyy-MM-dd'
                ),
                ''
            )
            when lmcdst = 2 then nvl(
                from_unixtime(
                    unix_timestamp(cast(cast(lmdtpo as int) as string), 'yyyyMMdd'),
                    'yyyy-MM-dd'
                ),
                ''
            )
        end as ec_date,
        nvl(
            last_day(
                from_unixtime(
                    unix_timestamp(cast(cast(lmdtnt as int) as string), 'yyyyMMdd'),
                    'yyyy-MM-dd'
                )
            ),
            ''
        ) as disb_month,
        case
            when lmcdst = 1 then nvl(
                last_day(
                    from_unixtime(
                        unix_timestamp(cast(cast(pt_date as int) as string), 'yyyyMMdd'),
                        'yyyy-MM-dd'
                    )
                ),
                ''
            )
            when lmcdst = 2 then nvl(
                last_day(
                    from_unixtime(
                        unix_timestamp(cast(cast(lmdtpo as int) as string), 'yyyyMMdd'),
                        'yyyy-MM-dd'
                    )
                ),
                ''
            )
        end as ec_month
    from ods.bke_visiona_lmbal_ss
    where pt_date <= "${date}"
        and pt_date = last_day(pt_date)
),
bv_mk_repayment_header as (
    select *,
        nvl(
            from_unixtime(
                unix_timestamp(cast(cast(lhdttr as int) as string), 'yyyyMMdd'),
                'yyyy-MM-dd'
            ),
            ''
        ) as repay_date,
        nvl(
            last_day(
                from_unixtime(
                    unix_timestamp(cast(cast(lhdttr as int) as string), 'yyyyMMdd'),
                    'yyyy-MM-dd'
                )
            ),
            ''
        ) as repay_month
    from ods.bke_visiona_lhish_ss
    where pt_date <= "${date}"
        and pt_date = last_day(pt_date)
        and pt_date = nvl(
            last_day(
                from_unixtime(
                    unix_timestamp(cast(cast(lhdttr as int) as string), 'yyyyMMdd'),
                    'yyyy-MM-dd'
                )
            ),
            ''
        )
),
bv_mk_repayment_detail as (
    select *,
        nvl(
            from_unixtime(
                unix_timestamp(cast(cast(lhdttr as int) as string), 'yyyyMMdd'),
                'yyyy-MM-dd'
            ),
            ''
        ) as repay_date,
        nvl(
            last_day(
                from_unixtime(
                    unix_timestamp(cast(cast(lhdttr as int) as string), 'yyyyMMdd'),
                    'yyyy-MM-dd'
                )
            ),
            ''
        ) as repay_month
    from ods.bke_visiona_lhisd_ss
    where pt_date <= "${date}"
        and pt_date = last_day(pt_date)
        and pt_date = nvl(
            last_day(
                from_unixtime(
                    unix_timestamp(cast(cast(lhdttr as int) as string), 'yyyyMMdd'),
                    'yyyy-MM-dd'
                )
            ),
            ''
        )
)