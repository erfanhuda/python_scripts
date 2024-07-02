WITH mk_com as (
     select 
     pt_date,
     product_type,
     tenor,
     payment_freq,
     tenor_in_month,
     int_real_rate,
     bucket_client,
     wo_flag,
     source,
     sum(cur_balance) as cur_balance,
     SUM(IF(SUBSTRING(loan_disbursement_date, 1, 7) = SUBSTRING(pt_Date, 1, 7), cur_balance, 0)) AS os_new_disburse,
     SUM(IF(SUBSTRING(loan_disbursement_date, 1, 7) <> SUBSTRING(pt_Date, 1, 7), cur_balance, 0)) as existing_balance,
     sum(wo_principal) as wo_principal,
     sum(clawback_principal) as clawback_principal,
     sum(wo_principal_mtd) as wo_principal_mtd,
     sum(clawback_principal_mtd) as clawback_principal_mtd,
     sum(wo_principal_mtd) - sum(clawback_principal_mtd) as gross_wo_principal_mtd
     from id_finpro.flow_rate_base_all_new
     where pt_date between date_format(add_months(date_add('{{start_date}}',-2),-1), 'yyyy-MM-01') and '{{end_date}}'
     group by pt_date,
     product_type,
     tenor,
     payment_freq,
     tenor_in_month,
     int_real_rate,
     bucket_client,
     wo_flag,
     source
)
, disburse_daily as (
     select * from (select pt_date, product_type, tenor,payment_freq,tenor_in_month, int_real_rate,sum(principal) as principal,sum(principal_mtd) as principal_mtd
          from id_finpro.flow_rate_base_disburse
          where pt_date between date_format(add_months(date_add('{{end_date}}',-2),-2), 'yyyy-MM-01') and '{{end_date}}'
          group by pt_date, product_type, tenor_in_month,tenor,payment_freq,int_real_rate) A
     where A.pt_date = last_day(A.pt_date)
)
, current_to_current as (
     select 
     a.product_type
     ,cast(a.tenor as integer) as tenor
     ,cast(a.tenor_in_month as integer) as tenor_in_month
     ,cast(a.payment_freq as integer) as payment_freq
     ,cast(a.int_real_rate as double) as int_real_rate
     ,'Current - Current' as matrix
     ,nvl(a.existing_balance,0) as existing_balance
     ,nvl(b.cur_balance,0) as cur_balance
     ,nvl(a.existing_balance,0)/nvl(b.cur_balance,0) as flow_rate
     ,a.pt_date
     from mk_com A --current
     join mk_com B --previous
     ON add_months(A.pt_date,-1) = B.pt_date AND 
       A.product_type = B.product_type AND
       A.tenor_in_month = B.tenor_in_month AND
       A.tenor = B.tenor AND
       A.payment_freq = B.payment_freq AND
       A.int_real_rate = B.int_real_rate
     where a.bucket_client=1
     and b.bucket_client=1
)
, disburse_to_current as(
     select 
     A.product_type
     ,cast(a.tenor as integer) as tenor
     ,cast(a.tenor_in_month as integer) as tenor_in_month
     ,cast(a.payment_freq as integer) as payment_freq
     ,cast(a.int_real_rate as double) as int_real_rate
     ,case when A.bucket_client =1 then 'Disburse - Current'
          when A.bucket_client = 2 then 'Disburse - M1'
          when A.bucket_client = 3 then 'Disburse - M2'
          when A.bucket_client = 4 then 'Disburse - M3'
          when A.bucket_client = 5 then 'Disburse - M4'
     end as matrix
     ,nvl(A.os_new_disburse,0) as num
     ,nvl(B.principal_mtd,0) as denom
     ,nvl(A.os_new_disburse,0)/nvl(B.principal_mtd,0) as flow_rate
     ,A.pt_date
     from mk_com A
     join disburse_daily B
     ON A.pt_date = B.pt_date AND 
        A.product_type = B.product_type AND
        A.tenor_in_month = B.tenor_in_month AND
        A.tenor = B.tenor AND
        A.payment_freq = B.payment_freq AND
        A.int_real_rate = B.int_real_rate
     where a.bucket_client between 1 and 3
)
, m1m4_raw_data as (
     -- No prev bucket movement
     select a.product_type
     , a.tenor
     , a.tenor_in_month
     , a.Payment_freq
     , a.int_real_rate
     , a.bucket_client
     , a.cur_balance
     , nvl(a.pt_date_prev, 0) as pt_date_prev
     , nvl(a.bucket_client_prev, 0) as bucket_client_prev
     , nvl(a.cur_balance_prev, 0) as cur_balance_prev
     , a.pt_date
     from (select 
          a.product_type
        , a.tenor
        , a.tenor_in_month
        , a.Payment_freq
        , a.int_real_rate
          , a.bucket_client
          , a.source
          , a.wo_flag
          , a.cur_balance
          , nvl(C.pt_date,0) as pt_date_prev
          , nvl(C.bucket_client,0) as bucket_client_prev
          , nvl(C.cur_balance,0) as cur_balance_prev
          , a.pt_date
          from (
               select a.pt_date
                    , a.product_type
                    , a.tenor
                    , a.tenor_in_month
                    , a.Payment_freq
                    , a.int_real_rate
                    , a.bucket_client
                    , a.source
                    , a.wo_flag
                    , a.cur_balance
                    , nvl(B.pt_date,0) as pt_date_prev
                    , nvl(B.bucket_client,0) as bucket_client_prev
                    , nvl(B.cur_balance,0) as cur_balance_prev
               from mk_com A --current
               left join mk_com B --previous
               ON add_months(A.pt_date,-1) = B.pt_date 
               AND (A.bucket_client-1) = B.bucket_client
               AND A.product_type = B.product_type
                AND A.tenor_in_month = B.tenor_in_month
                AND A.tenor = B.tenor
                AND A.payment_freq = B.payment_freq
                AND A.int_real_rate = B.int_real_rate) a
          left join mk_com C
          ON add_months(A.pt_date,-1) = C.pt_date 
          AND (A.bucket_client-2) = C.bucket_client
            AND A.product_type = c.product_type
            AND A.tenor_in_month = c.tenor_in_month
            AND A.tenor = c.tenor
            AND A.payment_freq = c.payment_freq
            AND A.int_real_rate = c.int_real_rate
          where a.bucket_client_prev = 0) A
     where a.bucket_client_prev = 0

     UNION ALL

     -- 2x bucket movement
     select 
          a.product_type
          , a.tenor
        , a.tenor_in_month
        , a.Payment_freq
        , a.int_real_rate
          , a.bucket_client
          , a.cur_balance
          , nvl(C.pt_date,0) as pt_date_prev
          , nvl(C.bucket_client,0) as bucket_client_prev
          , nvl(C.cur_balance,0) as cur_balance_prev
          , a.pt_date
     from (
          select a.pt_date
               , a.product_type
              , a.tenor
        , a.tenor_in_month
        , a.Payment_freq
        , a.int_real_rate
               , a.bucket_client
               , a.cur_balance
               , nvl(B.pt_date,0) as pt_date_prev
               , nvl(B.bucket_client,0) as bucket_client_prev
               , nvl(B.cur_balance,0) as cur_balance_prev
          from mk_com A --current
          left join mk_com B --previous
          ON add_months(A.pt_date,-1) = B.pt_date 
          AND (A.bucket_client-1) = B.bucket_client
          AND A.product_type = B.product_type
                AND A.tenor_in_month = B.tenor_in_month
                AND A.tenor = B.tenor
                AND A.payment_freq = B.payment_freq
                AND A.int_real_rate = B.int_real_rate) a
     inner join mk_com C
     ON add_months(A.pt_date,-1) = C.pt_date 
       AND (A.bucket_client-2) = C.bucket_client
       AND A.product_type = c.product_type
            AND A.tenor_in_month = c.tenor_in_month
            AND A.tenor = c.tenor
            AND A.payment_freq = c.payment_freq
            AND A.int_real_rate = c.int_real_rate
     where a.bucket_client_prev = 0

     UNION ALL

     -- 1x bucket movement
     select a.product_type
          , a.tenor
        , a.tenor_in_month
        , a.Payment_freq
        , a.int_real_rate
          , a.bucket_client
          , a.cur_balance
          , nvl(B.pt_date,0) as pt_date_prev
          , nvl(B.bucket_client,0) as bucket_client_prev
          , nvl(B.cur_balance,0) as cur_balance_prev
          , a.pt_date
     from mk_com A --current
     inner join mk_com B --previous
     ON add_months(A.pt_date,-1) = B.pt_date 
     AND (A.bucket_client-1) = B.bucket_client
     AND A.product_type = B.product_type
                AND A.tenor_in_month = B.tenor_in_month
                AND A.tenor = B.tenor
                AND A.payment_freq = B.payment_freq
                AND A.int_real_rate = B.int_real_rate
)
, m1_m4 as (
    select 
     product_type
     , cast(tenor as integer) as tenor
    , cast(tenor_in_month as integer) as tenor_in_month
    , cast(Payment_freq as integer) as Payment_freq
    , cast(int_real_rate as double) as int_real_rate
     , 
         -- Covering 1x bucket movement
     case when bucket_client_prev=1 and bucket_client=2 and wo_flag = 0 then 'Current - M1'
          when bucket_client_prev=2 and bucket_client=3 and wo_flag = 0 then 'M1 - M2'
          when bucket_client_prev=3 and bucket_client=4 and wo_flag = 0 then 'M2 - M3'
          when bucket_client_prev=4 and bucket_client=5 and wo_flag = 0 then 'M3 - M4'
          when bucket_client_prev=5 and bucket_client=6 and wo_flag = 0 then 'M4 - M5'
          when bucket_client_prev=6 and bucket_client=7 and wo_flag = 0 then 'M5 - M6'
          -- Covering 2x bucket movement
          when bucket_client_prev=1 and bucket_client=3 and wo_flag = 0 then 'Current - M2'
          when bucket_client_prev=2 and bucket_client=4 and wo_flag = 0 then 'M1 - M3'
          when bucket_client_prev=3 and bucket_client=5 and wo_flag = 0 then 'M2 - M4'
          when bucket_client_prev=4 and bucket_client=6 and wo_flag = 0 then 'M3 - M5'
          when bucket_client_prev=5 and bucket_client=7 and wo_flag = 0 then 'M4 - M6'
          -- Covering null last month
          when bucket_client_prev is null and bucket_client=3 and wo_flag = 0 then 'New Current - M2'
          when bucket_client_prev is null and bucket_client=4 and wo_flag = 0 then 'New M1 - M3'
          when bucket_client_prev is null and bucket_client=5 and wo_flag = 0 then 'New M2 - M4'
          when bucket_client_prev is null and bucket_client=6 and wo_flag = 0 then 'New M3 - M5'
          when bucket_client_prev is null and bucket_client=7 and wo_flag = 0 then 'New M4 - M6'
          -- Covering WO
          when bucket_client_prev = 5 and bucket_client=6 and wo_flag = 1 and source = 'WRITE_OFF' then 'M4 - WO'
          when bucket_client_prev = 7 and bucket_client=8 and wo_flag = 1 and source = 'WRITE_OFF' then 'M6 - WO'
          when bucket_client_prev = 5 and bucket_client=6 and wo_flag = 1 and source = 'ACTIVE' then 'ADDITIONAL M4 - WO'
          when bucket_client_prev = 7 and bucket_client=8 and wo_flag = 1 and source = 'ACTIVE' then 'ADDITIONAL M6 - WO'

     end as matrix,
     cast(cur_balance as bigint) as cur_balance,
     cast(cur_balance_prev as bigint) as prev_balance,
     cur_balance/cur_balance_prev as flow_rate
     ,pt_date
     from m1m4_raw_data
)
, wo_final as (
     -- 1x movement
     -- JF WO FROM MK WO
     select 
     a.product_type
     , cast(a.tenor as integer) as tenor
    , cast(a.tenor_in_month as integer) as tenor_in_month
    , cast(a.Payment_freq as integer) as Payment_freq
    , cast(a.int_real_rate as double) as int_real_rate
     ,'M4 - WO' as matrix
     ,nvl(a.gross_wo_principal_mtd,0) as numerator
     ,nvl(b.cur_balance,0) as denominator
     ,(nvl(a.gross_wo_principal_mtd,0)/nvl(b.cur_balance,0)) as flow_rate
     ,a.pt_date
     from mk_com A --current
     inner join mk_com B -- prev 
     ON add_months(A.pt_date,-1) = B.pt_date 
       AND A.product_type = B.product_type
       AND A.tenor_in_month = B.tenor_in_month
       and A.source = B.source
       and A.bucket_client - 1 = B.bucket_client
       and A.wo_flag = 1
       and A.source = 'WRITE_OFF'
     where b.product_type in ('SPL', 'BCL', 'SCL')

     UNION ALL

     -- JF WO FROM MK ACTIVE
     select 
     a.product_type
     , cast(a.tenor as integer) as tenor
    , cast(a.tenor_in_month as integer) as tenor_in_month
    , cast(a.Payment_freq as integer) as Payment_freq
    , cast(a.int_real_rate as double) as int_real_rate
     ,'M4 - WO' as matrix
     ,nvl(a.gross_wo_principal_mtd,0) as numerator
     ,nvl(b.cur_balance,0) as denominator
     ,(nvl(a.gross_wo_principal_mtd,0)/nvl(b.cur_balance,0)) as flow_rate
     ,a.pt_date
     from mk_com A --current
     inner join mk_com B -- prev 
     ON add_months(A.pt_date,-1) = B.pt_date 
       AND A.product_type = B.product_type
       AND A.tenor_in_month = B.tenor_in_month
       and A.source = B.source
       and A.bucket_client - 1 = B.bucket_client
       and A.wo_flag = 1
       and A.source = 'ACTIVE'
     where b.product_type in ('SPL', 'BCL', 'SCL')

     UNION ALL

     -- ODL WO FROM MK WO
     select 
     a.product_type
     , cast(a.tenor as integer) as tenor
    , cast(a.tenor_in_month as integer) as tenor_in_month
    , cast(a.Payment_freq as integer) as Payment_freq
    , cast(a.int_real_rate as double) as int_real_rate
     ,'M7 - WO' as matrix
     ,nvl(a.gross_wo_principal_mtd,0) as numerator
     ,nvl(b.cur_balance,0) as denominator
     ,(nvl(a.gross_wo_principal_mtd,0)/nvl(b.cur_balance,0)) as flow_rate
     ,a.pt_date
     from mk_com A --current
     inner join mk_com B -- prev 
     ON add_months(A.pt_date,-1) = B.pt_date 
       AND A.product_type = B.product_type
       AND A.tenor_in_month = B.tenor_in_month
       and A.source = B.source
       and A.bucket_client - 1 = B.bucket_client
       and A.wo_flag = 1
       and A.source = 'WRITE_OFF'
     where b.product_type not in ('SPL', 'BCL', 'SCL')

     UNION ALL

     -- ODL WO FROM MK ACTIVE
     select 
     a.product_type
     , cast(a.tenor as integer) as tenor
    , cast(a.tenor_in_month as integer) as tenor_in_month
    , cast(a.Payment_freq as integer) as Payment_freq
    , cast(a.int_real_rate as double) as int_real_rate
     ,'M7 - WO' as matrix
     ,nvl(a.gross_wo_principal_mtd,0) as numerator
     ,nvl(b.cur_balance,0) as denominator
     ,(nvl(a.gross_wo_principal_mtd,0)/nvl(b.cur_balance,0)) as flow_rate
     ,a.pt_date
     from mk_com A --current
     inner join mk_com B -- prev 
     ON add_months(A.pt_date,-1) = B.pt_date 
       AND A.product_type = B.product_type
       AND A.tenor_in_month = B.tenor_in_month
       and A.source = B.source
       and A.bucket_client - 1 = B.bucket_client
       and A.wo_flag = 1
       and A.source = 'ACTIVE'
     where b.product_type not in ('SPL', 'BCL', 'SCL')

     UNION ALL

     -- 2x movement
     -- JF WO FROM MK WO
     select 
     a.product_type
     , cast(a.tenor as integer) as tenor
    , cast(a.tenor_in_month as integer) as tenor_in_month
    , cast(a.Payment_freq as integer) as Payment_freq
    , cast(a.int_real_rate as double) as int_real_rate
     ,'M4 - WO' as matrix
     ,nvl(a.gross_wo_principal_mtd,0) as numerator
     ,nvl(b.cur_balance,0) as denominator
     ,(nvl(a.gross_wo_principal_mtd,0)/nvl(b.cur_balance,0)) as flow_rate
     ,a.pt_date
     from mk_com A --current
     inner join mk_com B -- prev 
     ON add_months(A.pt_date,-1) = B.pt_date 
       AND A.product_type = B.product_type
       AND A.tenor_in_month = B.tenor_in_month
       and A.source = B.source
       and A.bucket_client - 2 = B.bucket_client
       and A.wo_flag = 1
       and A.source = 'WRITE_OFF'
     where b.product_type in ('SPL', 'BCL', 'SCL')

     UNION ALL

     -- JF WO FROM MK ACTIVE
     select 
     a.product_type
     , cast(a.tenor as integer) as tenor
    , cast(a.tenor_in_month as integer) as tenor_in_month
    , cast(a.Payment_freq as integer) as Payment_freq
    , cast(a.int_real_rate as double) as int_real_rate
     ,'M4 - WO' as matrix
     ,nvl(a.gross_wo_principal_mtd,0) as numerator
     ,nvl(b.cur_balance,0) as denominator
     ,(nvl(a.gross_wo_principal_mtd,0)/nvl(b.cur_balance,0)) as flow_rate
     ,a.pt_date
     from mk_com A --current
     inner join mk_com B -- prev 
     ON add_months(A.pt_date,-1) = B.pt_date 
       AND A.product_type = B.product_type
       AND A.tenor_in_month = B.tenor_in_month
       and A.source = B.source
       and A.bucket_client - 2 = B.bucket_client
       and A.wo_flag = 1
       and A.source = 'ACTIVE'
     where b.product_type in ('SPL', 'BCL', 'SCL')

     UNION ALL

     -- ODL WO FROM MK WO
     select 
     a.product_type
     , cast(a.tenor as integer) as tenor
    , cast(a.tenor_in_month as integer) as tenor_in_month
    , cast(a.Payment_freq as integer) as Payment_freq
    , cast(a.int_real_rate as double) as int_real_rate
     ,'M7 - WO' as matrix
     ,nvl(a.gross_wo_principal_mtd,0) as numerator
     ,nvl(b.cur_balance,0) as denominator
     ,(nvl(a.gross_wo_principal_mtd,0)/nvl(b.cur_balance,0)) as flow_rate
     ,a.pt_date
     from mk_com A --current
     inner join mk_com B -- prev 
     ON add_months(A.pt_date,-1) = B.pt_date 
       AND A.product_type = B.product_type
       AND A.tenor_in_month = B.tenor_in_month
       and A.source = B.source
       and A.bucket_client - 2 = B.bucket_client
       and A.wo_flag = 1
       and A.source = 'WRITE_OFF'
     where b.product_type not in ('SPL', 'BCL', 'SCL')

     UNION ALL

     -- ODL WO FROM MK ACTIVE
     select 
     a.product_type
     , cast(a.tenor as integer) as tenor
    , cast(a.tenor_in_month as integer) as tenor_in_month
    , cast(a.Payment_freq as integer) as Payment_freq
    , cast(a.int_real_rate as double) as int_real_rate
     ,'M7 - WO' as matrix
     ,nvl(a.gross_wo_principal_mtd,0) as numerator
     ,nvl(b.cur_balance,0) as denominator
     ,(nvl(a.gross_wo_principal_mtd,0)/nvl(b.cur_balance,0)) as flow_rate
     ,a.pt_date
     from mk_com A --current
     inner join mk_com B -- prev 
     ON add_months(A.pt_date,-1) = B.pt_date 
       AND A.product_type = B.product_type
       AND A.tenor_in_month = B.tenor_in_month
       and A.source = B.source
       and A.bucket_client - 2 = B.bucket_client
       and A.wo_flag = 1
       and A.source = 'ACTIVE'
     where b.product_type not in ('SPL', 'BCL', 'SCL')
)
, end_result as (
     select * from m1_m4
     UNION ALL
     select * from wo_final
     UNION ALL
     select * from current_to_current
     UNION ALL
     select * from disburse_to_current
)

insert overwrite table id_finpro.flow_rate_all_client_summary_pricing
select * from end_result
where pt_date between '{{start_date}}' and '{{end_date}}'