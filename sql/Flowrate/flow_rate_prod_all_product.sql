WITH mk_com as (
     select 
     pt_date,
     product_type,
     bucket_product,
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
     from wo_com
     where pt_date between date_format(add_months(date_add('{{start_date}}',-2),-1), 'yyyy-MM-01') and '{{end_date}}'
     group by pt_date,
     product_type,
     bucket_product,
     wo_flag,
     source
)
, disburse_daily as (
     select * from (select pt_date, product_type,sum(principal) as principal,sum(principal_mtd) as principal_mtd
     from id_finpro.flow_rate_base_disburse
     where pt_date between date_format(add_months(date_add('{{end_date}}',-2),-2), 'yyyy-MM-01') and '{{end_date}}'
     group by pt_date, product_type) A
     where A.pt_date = last_day(A.pt_date)
)
, current_to_current as (
     select 
     a.product_type

     ,'Current - Current' as matrix
     ,nvl(a.existing_balance,0) as existing_balance
     ,nvl(b.cur_balance,0) as cur_balance
     ,nvl(a.existing_balance,0)/nvl(b.cur_balance,0) as flow_rate
     ,a.pt_date
     from mk_com A --current
     join mk_com B --previous
     ON add_months(A.pt_date,-1) = B.pt_date AND 
       A.product_type = B.product_type
     where a.bucket_product=1
     and b.bucket_product=1
)
, disburse_to_current as(
     select 
     A.product_type

     ,case when A.bucket_product =1 then 'Disburse - Current'
          when A.bucket_product = 2 then 'Disburse - M1'
          when A.bucket_product = 3 then 'Disburse - M2'
          when A.bucket_product = 4 then 'Disburse - M3'
          when A.bucket_product = 5 then 'Disburse - M4'
     end as matrix
     ,nvl(A.os_new_disburse,0) as num
     ,nvl(B.principal_mtd,0) as denom
     ,nvl(A.os_new_disburse,0)/nvl(B.principal_mtd,0) as flow_rate
     ,A.pt_date
     from mk_com A
     join disburse_daily B
     ON A.pt_date = B.pt_date AND 
        A.product_type = B.product_type
     where a.bucket_product between 1 and 3
)
, m1m4_raw_data as (
     -- No prev bucket movement
     select a.product_type
          , a.bucket_product
          , a.cur_balance
          , nvl(a.pt_date_prev, 0)
          , nvl(a.bucket_product_prev, 0)
          , nvl(a.cur_balance_prev, 0)
          , a.pt_date
     from (select 
          a.product_type
          , a.bucket_product
          , a.cur_balance
          , nvl(C.pt_date,0) as pt_date_prev
          , nvl(C.bucket_product,0) as bucket_product_prev
          , nvl(C.cur_balance,0) as cur_balance_prev
          , a.pt_date
          from (
               select a.pt_date
                    , a.product_type
                    , a.bucket_product
                    , a.cur_balance
                    , nvl(B.pt_date,0) as pt_date_prev
                    , nvl(B.bucket_product,0) as bucket_product_prev
                    , nvl(B.cur_balance,0) as cur_balance_prev
               from mk_com A --current
               left join mk_com B --previous
               ON add_months(A.pt_date,-1) = B.pt_date 
               AND (A.bucket_product-1) = B.bucket_product
               AND A.product_type = B.product_type
                ) a
          left join mk_com C
          ON add_months(A.pt_date,-1) = C.pt_date 
          AND (A.bucket_product-2) = C.bucket_product
            AND A.product_type = c.product_type
          where a.bucket_product_prev = 0)) A
     where a.bucket_product_prev = 0

     UNION ALL

     -- 2x bucket movement
     select 
          a.product_type
          , a.bucket_product
          , a.cur_balance
          , nvl(C.pt_date,0) as pt_date_prev
          , nvl(C.bucket_product,0) as bucket_product_prev
          , nvl(C.cur_balance,0) as cur_balance_prev
          , a.pt_date
     from (
          select a.pt_date
               , a.product_type
                     , a.bucket_product
               , a.cur_balance
               , nvl(B.pt_date,0) as pt_date_prev
               , nvl(B.bucket_product,0) as bucket_product_prev
               , nvl(B.cur_balance,0) as cur_balance_prev
          from mk_com A --current
          left join mk_com B --previous
          ON add_months(A.pt_date,-1) = B.pt_date 
          AND (A.bucket_product-1) = B.bucket_product
          AND A.product_type = B.product_type
                ) a
     inner join mk_com C
     ON add_months(A.pt_date,-1) = C.pt_date 
       AND (A.bucket_product-2) = C.bucket_product
       AND A.product_type = c.product_type
     where a.bucket_product_prev = 0

     UNION ALL

     -- 1x bucket movement
     select a.product_type
          , a.bucket_product
          , a.cur_balance
          , nvl(B.pt_date,0) as pt_date_prev
          , nvl(B.bucket_product,0) as bucket_product_prev
          , nvl(B.cur_balance,0) as cur_balance_prev
          , a.pt_date
     from mk_com A --current
     inner join mk_com B --previous
     ON add_months(A.pt_date,-1) = B.pt_date 
     AND (A.bucket_product-1) = B.bucket_product
     AND A.product_type = B.product_type
                
)
, m1_m4 as (
    select 
     product_type
     , 
         -- Covering 1x bucket movement
     case when bucket_product_prev=1 and bucket_product=2 and wo_flag = 0 then 'Current - M1'
          when bucket_product_prev=2 and bucket_product=3 and wo_flag = 0 then 'M1 - M2'
          when bucket_product_prev=3 and bucket_product=4 and wo_flag = 0 then 'M2 - M3'
          when bucket_product_prev=4 and bucket_product=5 and wo_flag = 0 then 'M3 - M4'
          when bucket_product_prev=5 and bucket_product=6 and wo_flag = 0 then 'M4 - M5'
          when bucket_product_prev=6 and bucket_product=7 and wo_flag = 0 then 'M5 - M6'
          -- Covering 2x bucket movement
          when bucket_product_prev=1 and bucket_product=3 and wo_flag = 0 then 'Current - M2'
          when bucket_product_prev=2 and bucket_product=4 and wo_flag = 0 then 'M1 - M3'
          when bucket_product_prev=3 and bucket_product=5 and wo_flag = 0 then 'M2 - M4'
          when bucket_product_prev=4 and bucket_product=6 and wo_flag = 0 then 'M3 - M5'
          when bucket_product_prev=5 and bucket_product=7 and wo_flag = 0 then 'M4 - M6'
          -- Covering null last month
          when bucket_product_prev is null and bucket_product=3 and wo_flag = 0 then 'New Current - M2'
          when bucket_product_prev is null and bucket_product=4 and wo_flag = 0 then 'New M1 - M3'
          when bucket_product_prev is null and bucket_product=5 and wo_flag = 0 then 'New M2 - M4'
          when bucket_product_prev is null and bucket_product=6 and wo_flag = 0 then 'New M3 - M5'
          when bucket_product_prev is null and bucket_product=7 and wo_flag = 0 then 'New M4 - M6'
          -- Covering WO
          when bucket_product_prev = 5 and bucket_product=6 and wo_flag = 1 and source = 'WRITE_OFF' then 'M4 - WO'
          when bucket_product_prev = 7 and bucket_product=8 and wo_flag = 1 and source = 'WRITE_OFF' then 'M6 - WO'
          when bucket_product_prev = 5 and bucket_product=6 and wo_flag = 1 and source = 'ACTIVE' then 'ADDITIONAL M4 - WO'
          when bucket_product_prev = 7 and bucket_product=8 and wo_flag = 1 and source = 'ACTIVE' then 'ADDITIONAL M6 - WO'

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
     ,'M4 - WO' as matrix
     ,nvl(a.gross_wo_principal_mtd,0) as numerator
     ,nvl(b.cur_balance,0) as denominator
     ,(nvl(a.gross_wo_principal_mtd,0)/nvl(b.cur_balance,0)) as flow_rate
     ,a.pt_date
     from mk_com A --current
     inner join mk_com B -- prev 
     ON add_months(A.pt_date,-1) = B.pt_date 
       AND A.product_type = B.product_type
       and A.source = B.source
       and A.bucket_product - 1 = B.bucket_product
       and A.wo_flag = 1
       and A.source = 'WRITE_OFF'
     where b.product_type in ('SPL', 'BCL', 'SCL')

     UNION ALL

     -- JF WO FROM MK ACTIVE
     select 
     a.product_type
     ,'M4 - WO' as matrix
     ,nvl(a.gross_wo_principal_mtd,0) as numerator
     ,nvl(b.cur_balance,0) as denominator
     ,(nvl(a.gross_wo_principal_mtd,0)/nvl(b.cur_balance,0)) as flow_rate
     ,a.pt_date
     from mk_com A --current
     inner join mk_com B -- prev 
     ON add_months(A.pt_date,-1) = B.pt_date 
       AND A.product_type = B.product_type
       and A.source = B.source
       and A.bucket_product - 1 = B.bucket_product
       and A.wo_flag = 1
       and A.source = 'ACTIVE'
     where b.product_type in ('SPL', 'BCL', 'SCL')

     UNION ALL

     -- ODL WO FROM MK WO
     select 
     a.product_type
     ,'M7 - WO' as matrix
     ,nvl(a.gross_wo_principal_mtd,0) as numerator
     ,nvl(b.cur_balance,0) as denominator
     ,(nvl(a.gross_wo_principal_mtd,0)/nvl(b.cur_balance,0)) as flow_rate
     ,a.pt_date
     from mk_com A --current
     inner join mk_com B -- prev 
     ON add_months(A.pt_date,-1) = B.pt_date 
       AND A.product_type = B.product_type
       and A.source = B.source
       and A.bucket_product - 1 = B.bucket_product
       and A.wo_flag = 1
       and A.source = 'WRITE_OFF'
     where b.product_type not in ('SPL', 'BCL', 'SCL')

     UNION ALL

     -- ODL WO FROM MK ACTIVE
     select 
     a.product_type
     ,'M7 - WO' as matrix
     ,nvl(a.gross_wo_principal_mtd,0) as numerator
     ,nvl(b.cur_balance,0) as denominator
     ,(nvl(a.gross_wo_principal_mtd,0)/nvl(b.cur_balance,0)) as flow_rate
     ,a.pt_date
     from mk_com A --current
     inner join mk_com B -- prev 
     ON add_months(A.pt_date,-1) = B.pt_date 
       AND A.product_type = B.product_type
       and A.source = B.source
       and A.bucket_product - 1 = B.bucket_product
       and A.wo_flag = 1
       and A.source = 'ACTIVE'
     where b.product_type not in ('SPL', 'BCL', 'SCL')

     -- 2x movement
     -- JF WO FROM MK WO
     select 
     a.product_type
     ,'M4 - WO' as matrix
     ,nvl(a.gross_wo_principal_mtd,0) as numerator
     ,nvl(b.cur_balance,0) as denominator
     ,(nvl(a.gross_wo_principal_mtd,0)/nvl(b.cur_balance,0)) as flow_rate
     ,a.pt_date
     from mk_com A --current
     inner join mk_com B -- prev 
     ON add_months(A.pt_date,-1) = B.pt_date 
       AND A.product_type = B.product_type
       and A.source = B.source
       and A.bucket_product - 2 = B.bucket_product
       and A.wo_flag = 1
       and A.source = 'WRITE_OFF'
     where b.product_type in ('SPL', 'BCL', 'SCL')

     UNION ALL

     -- JF WO FROM MK ACTIVE
     select 
     a.product_type
     ,'M4 - WO' as matrix
     ,nvl(a.gross_wo_principal_mtd,0) as numerator
     ,nvl(b.cur_balance,0) as denominator
     ,(nvl(a.gross_wo_principal_mtd,0)/nvl(b.cur_balance,0)) as flow_rate
     ,a.pt_date
     from mk_com A --current
     inner join mk_com B -- prev 
     ON add_months(A.pt_date,-1) = B.pt_date 
       AND A.product_type = B.product_type
       and A.source = B.source
       and A.bucket_product - 2 = B.bucket_product
       and A.wo_flag = 1
       and A.source = 'ACTIVE'
     where b.product_type in ('SPL', 'BCL', 'SCL')

     UNION ALL

     -- ODL WO FROM MK WO
     select 
     a.product_type
     ,'M7 - WO' as matrix
     ,nvl(a.gross_wo_principal_mtd,0) as numerator
     ,nvl(b.cur_balance,0) as denominator
     ,(nvl(a.gross_wo_principal_mtd,0)/nvl(b.cur_balance,0)) as flow_rate
     ,a.pt_date
     from mk_com A --current
     inner join mk_com B -- prev 
     ON add_months(A.pt_date,-1) = B.pt_date 
       AND A.product_type = B.product_type
       and A.source = B.source
       and A.bucket_product - 2 = B.bucket_product
       and A.wo_flag = 1
       and A.source = 'WRITE_OFF'
     where b.product_type not in ('SPL', 'BCL', 'SCL')

     UNION ALL

     -- ODL WO FROM MK ACTIVE
     select 
     a.product_type
     ,'M7 - WO' as matrix
     ,nvl(a.gross_wo_principal_mtd,0) as numerator
     ,nvl(b.cur_balance,0) as denominator
     ,(nvl(a.gross_wo_principal_mtd,0)/nvl(b.cur_balance,0)) as flow_rate
     ,a.pt_date
     from mk_com A --current
     inner join mk_com B -- prev 
     ON add_months(A.pt_date,-1) = B.pt_date 
       AND A.product_type = B.product_type
       and A.source = B.source
       and A.bucket_product - 2 = B.bucket_product
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

insert overwrite table id_finpro.flow_rate_all_client_summary_product
select * from end_result
where pt_date between '{{start_date}}' and '{{end_date}}'