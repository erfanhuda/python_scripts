-- FLOW RATE SCENARIO WHEN NO BUYBACK AND WRITE OFF RULES CHANGE TO 180 DAYS
--- PART M1 - M4 ---
WITH mk_com as (
     select 
     pt_date,
     product_type,
     tenor,
     case when payment_freq = 0 then 1
          else payment_freq end as payment_freq,
     bucket_client,
     sum(cur_balance) as cur_balance,
     SUM(IF(SUBSTRING(loan_disbursement_date, 1, 7) = SUBSTRING(pt_Date, 1, 7), cur_balance, 0)) AS os_new_disburse,
     SUM(IF(SUBSTRING(loan_disbursement_date, 1, 7) <> SUBSTRING(pt_Date, 1, 7), cur_balance, 0)) as existing_balance
     from id_finpro.flow_rate_base_active_combine_wo_120_buyback
     -- where pt_date between date_format(add_months(date_add(current_date(),-2),-1), 'yyyy-MM-01') and current_date()
     where pt_date between date_format(add_months(date_add('{{start_date}}',-2),-1), 'yyyy-MM-01') and '{{end_date}}'
     group by 
     pt_date,
     product_type,
     tenor,
     case when payment_freq = 0 then 1
          else payment_freq end,
     bucket_client
     
), wo_clawback_daily as (
     select pt_date,
       product_type,
       tenor,
       payment_freq,
       sum(wo_principal) as wo_principal,
       sum(clawback_principal) as clawback_principal,
       sum(wo_principal_mtd) as wo_principal_mtd,
       sum(clawback_principal_mtd) as clawback_principal_mtd,
       sum(wo_principal_mtd) - sum(clawback_principal_mtd) as gross_wo_principal_mtd 
     from id_finpro.flow_rate_base_wo
     -- where pt_date between date_format(add_months(date_add(current_date(),-2),-1), 'yyyy-MM-01') and current_date()
     where pt_date between date_format(add_months(date_add('{{start_date}}',-2),-1), 'yyyy-MM-01') and '{{end_date}}'
     group by
       pt_date,
       product_type,
       tenor,
       payment_freq
), disburse_daily as (
     select pt_date, product_type, tenor, payment_freq, sum(principal) as principal,sum(principal_mtd) as principal_mtd
     from id_finpro.flow_rate_base_disburse 
     -- where pt_date between date_format(add_months(date_add(current_date(),-2),-1), 'yyyy-MM-01') and current_date()
     where pt_date between date_format(add_months(date_add('{{start_date}}',-2),-1), 'yyyy-MM-01') and '{{end_date}}'
     group by pt_date, product_type, tenor, payment_freq
)
, m1m4_raw_data as (
     select 
          a.pt_date
          , a.product_type
          , a.tenor
          , a.payment_freq
          , a.bucket_client
          , a.cur_balance
          , nvl(C.pt_date,0) as pt_date_prev
          , nvl(C.bucket_client,0) as bucket_client_prev
          , nvl(C.cur_balance,0) as cur_balance_prev
     from (
               select a.pt_date
               , a.product_type
               , a.tenor
               , a.payment_freq
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
          AND A.tenor = B.tenor 
          AND A.payment_freq = B.payment_freq) a
     inner join mk_com C
     ON add_months(A.pt_date,-1) = C.pt_date 
       AND (A.bucket_client-2) = C.bucket_client
       AND A.product_type = C.product_type
       AND A.tenor = C.tenor 
       AND A.payment_freq = C.payment_freq
     where a.bucket_client_prev = 0

     UNION ALL

     select a.pt_date
          , a.product_type
          , a.tenor
          , a.payment_freq
          , a.bucket_client
          , a.cur_balance
          , nvl(B.pt_date,0) as pt_date_prev
          , nvl(B.bucket_client,0) as bucket_client_prev
          , nvl(B.cur_balance,0) as cur_balance_prev
     from mk_com A --current
     inner join mk_com B --previous
     ON add_months(A.pt_date,-1) = B.pt_date 
     AND (A.bucket_client-1) = B.bucket_client
     AND A.product_type = B.product_type
     AND A.tenor = B.tenor 
     AND A.payment_freq = B.payment_freq
) 
, m1_m4 as (
     select pt_date,product_type,tenor,payment_freq,

          -- Covering normal day past due
     case when bucket_client_prev=1 and bucket_client=2 then 'Current - M1'
          when bucket_client_prev=2 and bucket_client=3 then 'M1 - M2'
          when bucket_client_prev=3 and bucket_client=4 then 'M2 - M3'
          when bucket_client_prev=4 and bucket_client=5 then 'M3 - M4'
          when bucket_client_prev=5 and bucket_client=6 then 'M4 - M5'
          when bucket_client_prev=6 and bucket_client=7 then 'M5 - M6'
          -- when bucket_client_prev=7 and bucket_client=8 then 'M6 - WO'
          -- Covering skipped day past due
          when bucket_client_prev=1 and bucket_client=3 then 'Current - M2'
          when bucket_client_prev=2 and bucket_client=4 then 'M1 - M3'
          when bucket_client_prev=3 and bucket_client=5 then 'M2 - M4'
          when bucket_client_prev=4 and bucket_client=6 then 'M3 - M5'
          when bucket_client_prev=5 and bucket_client=7 then 'M4 - M6'
          -- when bucket_client_prev=6 and bucket_client=8 then 'M5 - WO'
     end as matrix,
     nvl(cur_balance,0) as cur_balance,
     nvl(cur_balance_prev,0) as prev_balance,
     nvl(cur_balance/cur_balance_prev,0) as flow_rate
     from m1m4_raw_data
)

--- PART M5 to WO ---
, wo_date_series as (
     select date_add(concat(date_format('{{start_date}}', 'yyyy-MM'), '-01'), a.pos) as dateseries
     from (select posexplode(split(repeat("o", datediff(last_day('{{end_date}}'),concat(date_format('{{start_date}}', 'yyyy-MM'), '-01'))), "o"))) a
)
, wo_fly_wo_120 as (
    select B.product_code, B.product_type, B.loan_maturity_date, B.loan_disbursement_date, B.tenor, B.tenor_in_month, B.payment_freq 
    , A.dateseries as pt_date,
    sum(nvl(B.cur_balance,0)) over (partition by substring(A.dateseries,1,7), B.product_code, B.product_type, B.loan_maturity_date, B.loan_disbursement_date, B.tenor, B.tenor_in_month, B.payment_freq 
    order by A.dateseries rows unbounded preceding) as wo_principal_mtd
     from wo_date_series A 
     left join id_finpro.flow_rate_base_active_combine_wo_120_buyback B
     on A.dateseries = B.pt_date
     where B.bucket_client = 8
)
, m7_final_wo_180_buyback as (
     select a.pt_date,a.product_type,cast(a.tenor as double) as tenor,cast(a.payment_freq as double) as payment_freq,
     'M6 - WO' as matrix,
     sum(a.wo_principal_mtd) as nom,
     sum(b.cur_balance) as denom,
     (sum(a.wo_principal_mtd)/sum(b.cur_balance)) as flow_rate
     from wo_fly_wo_120 A
     inner join mk_com B
     ON add_months(A.pt_date,-1) = B.pt_date 
     AND A.product_type = B.product_type
     AND A.tenor = B.tenor
     AND A.payment_freq = B.payment_freq
     where b.bucket_client=7
     group by a.pt_date,a.product_type, cast(a.tenor as double),cast(a.payment_freq as double)
)

-- PART Current to Current --
, current_to_current as (
     select a.pt_date,a.product_type,a.tenor,a.payment_freq,
     'Current - Current' as matrix,
     a.existing_balance
     ,b.cur_balance, (a.existing_balance/b.cur_balance) as flow_rate
     from mk_com A --current
     full join mk_com B --previous
     ON add_months(A.pt_date,-1) = B.pt_date AND 
       A.product_type = B.product_type AND
       A.tenor = B.tenor and
       A.payment_freq = B.payment_freq
     where a.bucket_client=1
     and b.bucket_client=1
     
)

-- PART Disburse to Current --
, disburse_to_current as(
     select A.pt_date,A.product_type,A.tenor,A.payment_freq,
     case when A.bucket_client =1 then 'Disburse - Current'
          when A.bucket_client = 2 then 'Disburse - M1'
          when A.bucket_client = 3 then 'Disburse - M2'
          when A.bucket_client = 4 then 'Disburse - M3'
          when A.bucket_client = 5 then 'Disburse - M4'
     end as matrix,
     A.os_new_disburse as nom,
     B.principal_mtd as denom,
     A.os_new_disburse/B.principal_mtd as flow_rate
     from mk_com A
     full join disburse_daily B
     ON A.pt_date = B.pt_date AND 
          A.product_type = B.product_type AND
          A.tenor = B.tenor and
          a.payment_freq = B.payment_freq
     where a.bucket_client between 1 and 3
)
, end_result as (
     select * from m1_m4
     -- UNION ALL
     -- select * from m7_final_buyback
     UNION ALL
     select * from m7_final_wo_180_buyback
     UNION ALL
     select * from current_to_current
     UNION ALL
     select * from disburse_to_current
)

select * from end_result
where pt_date between '{{start_date}}' and '{{end_date}}';
