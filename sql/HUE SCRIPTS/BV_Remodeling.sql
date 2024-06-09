with date_series as (
    select *
    from id_finpro.calendar_month_series
),
bv_mk_full as (
    select pt_date,
        lmnoac,
        lmcdpl,
        lmnotp,
        lmsnam,
        lmdtnt,
        lmdtcl,
        lmcdst,
        lmdtco,
        lmamcl,
        lmnopd,
        lmamcb,
        row_number() over (
            partition by pt_date,
            lmnoac
            order by lmnoac,
                pt_date
        ) as rn
    from ods.bke_visiona_lmbal_ss
    where pt_date = last_day(pt_date)
        and pt_date <= "${pt_date}"
),
bv_base_data as (
    select *,
        case
            when lmcdst = 1
            and lmnopd = 0 then 1
            when lmcdst = 1
            and lmnopd between 1 and 30 then 2
            when lmcdst = 1
            and lmnopd between 31 and 60 then 3
            when lmcdst = 1
            and lmnopd between 61 and 90 then 4
            when lmcdst = 1
            and lmnopd > 90 then 5
            when lmcdst = 2 then 0
        end as bucket_acct,
        case
            when lmcdst = 1
            and lmnopd between 0 and 30 then 1
            when lmcdst = 1
            and lmnopd between 31 and 90 then 2
            when lmcdst in (1, 2)
            and lmnopd > 90 then 3
            when lmcdst = 2 then 0
        end as stage_acct,
        case
            when lmcdst = 1
            and lmnopd = 0 then 1
            when lmcdst = 1
            and lmnopd between 1 and 90 then 2
            when lmcdst = 1
            and lmnopd between 91 and 120 then 3
            when lmcdst = 1
            and lmnopd between 121 and 180 then 4
            when lmcdst = 1
            and lmnopd > 180 then 5
            when lmcdst = 2 then 0
        end as col_acct
    from bv_mk_full
),
cif_data as (
    select *,
        max(bucket_acct) over (
            partition by pt_date,
            lmsnam
            order by lmsnam,
                pt_date
        ) as final_bucket_cif,
        max(stage_acct) over (
            partition by pt_date,
            lmsnam
            order by lmsnam,
                pt_date
        ) as final_stage_cif,
        max(col_acct) over (
            partition by pt_date,
            lmsnam
            order by lmsnam,
                pt_date
        ) as final_col_cif
    from bv_base_data
),
bucket_movement as (
    select *,
        cast(add_months(pt_date, 12) as date) as yoy_date,
        case
            when lead(final_bucket_cif, 12) over w < final_bucket_cif then max(final_bucket_cif) over w
            else lead(final_bucket_cif, 12) over w
        end as YoY_Bucket_Cif,
        case
            when lead(final_stage_cif, 12) over w < final_stage_cif then max(final_stage_cif) over w
            else lead(final_stage_cif, 12) over w
        end as YoY_Stage_Cif,
        case
            when lead(final_col_cif, 12) over w < final_col_cif then max(final_col_cif) over w
            else lead(final_col_cif, 12) over w
        end as YoY_Col_Cif -- 	lead(final_stage_cif, 12) over (partition by lmnoac, lmsnam order by lmnoac, lmsnam, pt_date) as YoY_Stage_Cif,
        -- 	lead(final_col_cif, 12) over (partition by lmnoac, lmsnam order by lmnoac, lmsnam, pt_date) as YoY_Col_Cif
    from cif_data window w as (
            partition by lmnoac,
            lmsnam
            order by lmnoac,
                lmsnam,
                pt_date range between unbounded preceding and unbounded following
        )
),
pd_aggregate as (
    select pt_date,
        yoy_date,
        lmcdpl as product_code,
        lmnotp as tenor,
        case
            when lmcdpl in ('K10', 'K11', 'KR1', 'K01', 'K09') then 'Channeling'
            when lmcdpl in (
                '274',
                'DL1',
                'DL2',
                'I02',
                'IR1',
                'M02',
                'M03',
                'MR1',
                'MR2',
                'MR3'
            ) then 'Commercial'
            when lmcdpl in (
                '271',
                '272',
                'A01',
                'A05',
                'A09',
                'A17',
                'A25',
                'A29',
                'I01',
                'K13',
                'K17',
                'K18',
                'K20',
                'K21',
                'K24',
                'K27',
                'K28',
                'A13',
                'A26',
                'A02',
                'A14',
                'K25',
                'B03'
            ) then 'Cooperatives'
            when lmcdpl in (
                'K05',
                'K15',
                'K16',
                'K19',
                'K29',
                'KR2',
                'KR3',
                'P04',
                'S14',
                'S18',
                'S99',
                'SS5',
                'S11',
                'SS1'
            ) then 'Other Consumer'
        end AS PD_SEGMENT,
        case
            when lmcdpl in (
                'K10',
                'K11',
                'KR1',
                'DL1',
                'IR1',
                'M02',
                'M03',
                'MR3',
                '271',
                'A01',
                'A09',
                'A17',
                'A29',
                'K05',
                'K15',
                'K16',
                'K19',
                'K29',
                'KR2',
                'KR3',
                'K01',
                'K09',
                'A13',
                'A26',
                'A02',
                'A14',
                'K25',
                'P04',
                'S14',
                'S18',
                'S99',
                'SS5',
                'S11',
                'SS1',
                'B03'
            ) then 'UNSECURED'
            when lmcdpl in (
                '274',
                'DL2',
                'I02',
                'MR1',
                'MR2',
                '272',
                'A05',
                'A25',
                'I01',
                'K13',
                'K17',
                'K18',
                'K20',
                'K21',
                'K24',
                'K27',
                'K28'
            ) then 'SECURED'
        end AS LGD_SEGMENT,
        final_bucket_cif as bucket_from,
        YoY_Bucket_Cif as bucket_to,
        case
            when YoY_Bucket_Cif = 5 then 1
            else 0
        end as default_Flag,
        count(lmnoac) as cum_account,
        count(distinct lmsnam) as cum_cif,
        sum(lmamcb) as cum_balance
    from bucket_movement
    where YoY_Bucket_Cif is not null
    group by pt_date,
        yoy_date,
        lmcdpl,
        lmnotp,
        final_bucket_cif,
        YoY_Bucket_Cif,
        case
            when lmcdpl in ('K10', 'K11', 'KR1', 'K01', 'K09') then 'Channeling'
            when lmcdpl in (
                '274',
                'DL1',
                'DL2',
                'I02',
                'IR1',
                'M02',
                'M03',
                'MR1',
                'MR2',
                'MR3'
            ) then 'Commercial'
            when lmcdpl in (
                '271',
                '272',
                'A01',
                'A05',
                'A09',
                'A17',
                'A25',
                'A29',
                'I01',
                'K13',
                'K17',
                'K18',
                'K20',
                'K21',
                'K24',
                'K27',
                'K28',
                'A13',
                'A26',
                'A02',
                'A14',
                'K25',
                'B03'
            ) then 'Cooperatives'
            when lmcdpl in (
                'K05',
                'K15',
                'K16',
                'K19',
                'K29',
                'KR2',
                'KR3',
                'P04',
                'S14',
                'S18',
                'S99',
                'SS5',
                'S11',
                'SS1'
            ) then 'Other Consumer'
        end,
        case
            when lmcdpl in (
                'K10',
                'K11',
                'KR1',
                'DL1',
                'IR1',
                'M02',
                'M03',
                'MR3',
                '271',
                'A01',
                'A09',
                'A17',
                'A29',
                'K05',
                'K15',
                'K16',
                'K19',
                'K29',
                'KR2',
                'KR3',
                'K01',
                'K09',
                'A13',
                'A26',
                'A02',
                'A14',
                'K25',
                'P04',
                'S14',
                'S18',
                'S99',
                'SS5',
                'S11',
                'SS1',
                'B03'
            ) then 'UNSECURED'
            when lmcdpl in (
                '274',
                'DL2',
                'I02',
                'MR1',
                'MR2',
                '272',
                'A05',
                'A25',
                'I01',
                'K13',
                'K17',
                'K18',
                'K20',
                'K21',
                'K24',
                'K27',
                'K28'
            ) then 'SECURED'
        end,
        case
            when YoY_Bucket_Cif = 5 then 1
            else 0
        end
),
base_odr as (
    select pt_date,
        yoy_date,
        pd_segment,
        lgd_segment,
        bucket_from,
        bucket_to,
        sum(cum_account) as sum_id,
        sum(cum_cif) as sum_cif,
        sum(cum_balance) as sum_balance
    from pd_aggregate
    where pd_segment is not null
    group by pt_date,
        yoy_date,
        pd_segment,
        lgd_segment,
        bucket_from,
        bucket_to
),
base_pit_pd as (
    select pt_date,
        yoy_date,
        pd_segment,
        lgd_segment,
        bucket_from,
        bucket_to,
        sum(cum_account) as sum_id,
        sum(cum_cif) as sum_cif,
        sum(cum_balance) as sum_balance
    from pd_aggregate
    where pd_segment is not null
    group by pt_date,
        yoy_date,
        pd_segment,
        lgd_segment,
        bucket_from,
        bucket_to
),
base_ttc_pd as (
    select *,
        sum(sum_id) over w,
        sum(sum_cif) over w,
        sum(sum_balance) over w,
        sum(sum_id) over w / sum(sum_id) over (
            partition by pd_segment,
            lgd_segment,
            bucket_from
            order by pd_segment,
                lgd_segment,
                bucket_from,
                bucket_to,
                pt_date
        ) as ttc_acct
    from base_pit_pd window w as (
            partition by pd_segment,
            lgd_segment,
            bucket_from,
            bucket_to
            order by pd_segment,
                lgd_segment,
                bucket_from,
                bucket_to,
                pt_date
        )
),
pit_pd as (
    select *,
        sum_id / sum(sum_id) over w as pit_acct,
        sum_cif / sum(sum_cif) over w as pit_cif,
        sum_balance / sum(sum_balance) over w as pit_balance
    from base_pit_pd window w as (
            partition by pd_segment,
            lgd_segment,
            bucket_from,
            pt_date,
            yoy_date
            order by pd_segment,
                lgd_segment,
                bucket_from,
                pt_date,
                yoy_date
        )
)
select *
from base_ttc_pd;