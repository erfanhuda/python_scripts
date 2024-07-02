with mk as (
    select loan_no,client_no,group_client_no,product_code,
        case 
            when product_code='101' then 'SPL' 
            when product_code='102' then 'SPL' 
            when product_code='103' then 'BCL' 
            when product_code='104' then 'BCL' 
            when product_code='105' then 'SCL' 
            when product_code='106' then 'KPL' 
            when product_code='107' then 'EML' 
            when product_code='108' then 'RCL' 
            when product_code='109' then 'AKL' 
            when product_code='110' then 'EAL' 
            when product_code='111' then 'PYL' 
            when product_code='112' then 'UDL' 
            when product_code='115' then 'APL' 
            when product_code='C01' then 'APL' 
            when product_code='SC1' then 'SCF' 
            when product_code='SC2' then 'SCF' 
            when product_code='M05' then 'SME'
        else product_type end as product_type
        ,int_real_rate
        ,tenor
        ,nvl(tenor_in_month
        ,case when product_code in ('108','109','110') then round(months_between(loan_maturity_date,loan_disbursement_date),0) else tenor end) as tenor_in_month
        ,nvl(payment_freq,1) as payment_freq
        ,day_past_due
        ,cur_balance
        ,loan_disbursement_date
        ,loan_maturity_date
        ,import_source
        ,pt_date
    from dm.rep_fin_reg_com_master_kredit_ss_d
    where (import_source='Digibank' or product_code in ('C01','SC1','SC2','M05'))
    and pt_date between '{{start_date}}' and '{{end_date}}'
    -- and pt_date = date_add(current_date(),-1)
) 
,mk2 as (
    select loan_no,client_no,product_code
        ,product_type
        ,day_past_due
        ,cur_balance
        ,import_source
        ,loan_maturity_date
        ,loan_disbursement_date
        ,tenor
        ,tenor_in_month
        ,payment_freq
        ,int_real_rate
        ,max(day_past_due) over (partition by pt_date,client_no) as day_past_due_client
        ,max(day_past_due) over (partition by pt_date,group_client_no) as day_past_due_group_client
        ,max(day_past_due) over (partition by pt_date,client_no,product_type) as day_past_due_product
        ,pt_date
    from mk

)
, mk3 as (
    select product_code,
        product_type
        ,loan_maturity_date
        ,loan_disbursement_date
        ,tenor
        ,tenor_in_month
        ,payment_freq
        ,int_real_rate
        ,day_past_due
        ,day_past_due_client
        ,day_past_due_product
        ,case when day_past_due = 0 then 1
              when day_past_due between 1 and 30 then 2
              when day_past_due between 31 and 60 then 3
              when day_past_due between 61 and 90 then 4
              when day_past_due between 91 and 120 then 5
              when day_past_due between 121 and 150 then 6
              when day_past_due between 151 and 180 then 7 end as bucket_loan
        ,case when day_past_due_client = 0 then 1
              when day_past_due_client between 1 and 30 then 2
              when day_past_due_client between 31 and 60 then 3
              when day_past_due_client between 61 and 90 then 4
              when day_past_due_client between 91 and 120 then 5      
              when day_past_due_client between 121 and 150 then 6      
              when day_past_due_client between 151 and 180 then 7 end as bucket_client
            
        ,case when day_past_due_product = 0 then 1
              when day_past_due_product between 1 and 30 then 2
              when day_past_due_product between 31 and 60 then 3
              when day_past_due_product between 61 and 90 then 4
              when day_past_due_product between 91 and 120 then 5
              when day_past_due_product between 121 and 150 then 6
              when day_past_due_product between 151 and 180 then 7 end as bucket_product
        ,sum(cur_balance) as cur_balance
        ,pt_date
    from mk2
    group by product_code,
        product_type
        ,loan_maturity_date
        ,loan_disbursement_date
        ,tenor
        ,tenor_in_month
        ,payment_freq
        ,int_real_rate
        ,day_past_due
        ,day_past_due_client
        ,day_past_due_product
        ,case when day_past_due = 0 then 1
              when day_past_due between 1 and 30 then 2
              when day_past_due between 31 and 60 then 3
              when day_past_due between 61 and 90 then 4
              when day_past_due between 91 and 120 then 5
              when day_past_due between 121 and 150 then 6
              when day_past_due between 151 and 180 then 7 end
        ,case when day_past_due_client = 0 then 1
              when day_past_due_client between 1 and 30 then 2
              when day_past_due_client between 31 and 60 then 3
              when day_past_due_client between 61 and 90 then 4
              when day_past_due_client between 91 and 120 then 5      
              when day_past_due_client between 121 and 150 then 6      
              when day_past_due_client between 151 and 180 then 7 end      
        ,case when day_past_due_product = 0 then 1
              when day_past_due_product between 1 and 30 then 2
              when day_past_due_product between 31 and 60 then 3
              when day_past_due_product between 61 and 90 then 4
              when day_past_due_product between 91 and 120 then 5
              when day_past_due_product between 121 and 150 then 6
              when day_past_due_product between 151 and 180 then 7 end
      ,pt_date
)
insert overwrite table id_finpro.flow_rate_base_active partition (pt_date)
select * from mk3;
