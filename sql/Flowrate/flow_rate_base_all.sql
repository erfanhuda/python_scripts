with com_mk as (
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
        ,nvl(tenor_in_month,case when product_code in ('108','109','110') then round(months_between(loan_maturity_date,loan_disbursement_date),0) else tenor end) as tenor_in_month
        ,nvl(payment_freq,1) as payment_freq
        ,day_past_due
        ,cur_balance
        , 0 as write_off_principal
        , 0 as write_off_interest
        , 0 as clawback_principal
        , 0 as clawback_interest
        ,loan_disbursement_date
        ,loan_maturity_date
        ,loan_disb
        ,import_source
        ,'ACTIVE' as source
        ,pt_date
    from dm.rep_fin_reg_com_master_kredit_ss_d
    where (import_source='Digibank' or product_code in ('C01','SC1','SC2','M05'))
    and pt_date between '{{start_date}}' and '{{end_date}}'

UNION ALL

select 
     loan_no,client_no,group_client_no,product_code,
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
        else product_type 
    end as product_type ,
    int_real_rate,
    tenor,
        nvl(
            tenor_in_month,
case
                when product_code in ('108', '109', '110') then round(
                    months_between(loan_maturity_date, loan_disbursement_date),
                    0
                )
                else tenor
            end
        ) as tenor_in_month 
,
        nvl(payment_freq, 1) as payment_freq,
    day_past_due,
    0 as cur_balance,
    case when written_off_date = pt_date then write_off_principal
         else 0 end as wo_principal,
    case when written_off_date = pt_date then write_off_interest
         else 0 end as wo_interest,
    case when clawback_date = pt_date then clawback_interest
         else 0 end as clawback_interest,
    case when clawback_date = pt_date then clawback_principal
         else 0 end as clawback_principal,
    loan_disbursement_date,
    loan_maturity_date,
    loan_disb,
    import_source,
    'WRITE_OFF',
    pt_date
    from dm.rep_fin_reg_db_master_kredit_write_off_ss_d
    where 
    pt_date between '{{start_date}}' and '{{end_date}}')
, com_bucket as (
    select *
    ,IF(SUBSTRING(loan_disbursement_date, 1, 7) = SUBSTRING(pt_Date, 1, 7), cur_balance,0) AS os_new_disburse
    ,IF(SUBSTRING(loan_disbursement_date, 1, 7) <> SUBSTRING(pt_Date, 1, 7),cur_balance,0) as existing_balance
    ,max(day_past_due) over (partition by pt_date,client_no,source) as day_past_due_client
    ,max(day_past_due) over (partition by pt_date,group_client_no,source) as day_past_due_group_client
    ,max(day_past_due) over (partition by pt_date,client_no,product_type,source) as day_past_due_product
    from com_mk
)
, prefinal_base as (

select 
    product_code
    ,product_type
    ,loan_maturity_date
    ,loan_disbursement_date
    ,tenor
    ,tenor_in_month
    ,payment_freq
    ,int_real_rate
    ,source
    ,day_past_due
    ,day_past_due_client
    ,day_past_due_product
    ,case   when day_past_due = 0 then 1
                when day_past_due between 1 and 30 then 2
                when day_past_due between 31 and 60 then 3
                when day_past_due between 61 and 90 then 4
                when day_past_due between 91 and 120 then 5
                when day_past_due between 121 and 150 then 6
                when day_past_due between 151 and 180 then 7
                when day_past_due > 180 then 8
                else 0 end as bucket_loan
    ,case   when day_past_due_client = 0 then 1
                when day_past_due_client between 1 and 30 then 2
                when day_past_due_client between 31 and 60 then 3
                when day_past_due_client between 61 and 90 then 4
                when day_past_due_client between 91 and 120 then 5
                when day_past_due_client between 121 and 150 then 6
                when day_past_due_client between 151 and 180 then 7
                when day_past_due_client > 180 then 8
                else 0 end as bucket_client
    ,case   when day_past_due_product = 0 then 1
                when day_past_due_product between 1 and 30 then 2
                when day_past_due_product between 31 and 60 then 3
                when day_past_due_product between 61 and 90 then 4
                when day_past_due_product between 91 and 120 then 5
                when day_past_due_product between 121 and 150 then 6
                when day_past_due_product between 151 and 180 then 7
                when day_past_due_product > 180 then 8
                else 0 end as bucket_product
    ,case when product_type in ('SPL', 'BCL', 'SCL') and day_past_due > 120 OR source = 'WRITE_OFF' then 1
          when product_type not in ('SPL', 'BCL', 'SCL') and day_past_due > 180 OR source = 'WRITE_OFF' then 1
          else 0 end as wo_flag
    ,sum(nvl(cur_balance,0)) as cur_balance
    ,sum(nvl(os_new_disburse,0)) as os_new_disburse
    ,sum(nvl(existing_balance,0)) as existing_balance
    ,sum(case when product_type in ('SPL', 'BCL', 'SCL') and day_past_due > 120 AND source = 'ACTIVE' then cur_balance
          when product_type not in ('SPL', 'BCL', 'SCL') and day_past_due > 180 AND source = 'ACTIVE' then cur_balance
          else nvl(write_off_principal,0) end) as wo_principal
    ,sum(nvl(write_off_interest,0)) as wo_interest
    ,sum(nvl(clawback_principal,0)) as clawback_principal
    ,sum(nvl(clawback_interest,0)) as clawback_interest
    ,pt_date
from com_bucket
group by product_code
    ,product_type
    ,loan_maturity_date
    ,loan_disbursement_date
    ,tenor
    ,tenor_in_month
    ,payment_freq
    ,int_real_rate
    ,source
    ,day_past_due
    ,day_past_due_client
    ,day_past_due_product
    ,case   when day_past_due = 0 then 1
            when day_past_due between 1 and 30 then 2
            when day_past_due between 31 and 60 then 3
            when day_past_due between 61 and 90 then 4
            when day_past_due between 91 and 120 then 5
            when day_past_due between 121 and 150 then 6
            when day_past_due between 151 and 180 then 7
            when day_past_due > 180 then 8
            else 0 end
    ,case   when day_past_due_client = 0 then 1
            when day_past_due_client between 1 and 30 then 2
            when day_past_due_client between 31 and 60 then 3
            when day_past_due_client between 61 and 90 then 4
            when day_past_due_client between 91 and 120 then 5
            when day_past_due_client between 121 and 150 then 6
            when day_past_due_client between 151 and 180 then 7
            when day_past_due_client > 180 then 8
            else 0 end
    ,case   when day_past_due_product = 0 then 1
            when day_past_due_product between 1 and 30 then 2
            when day_past_due_product between 31 and 60 then 3
            when day_past_due_product between 61 and 90 then 4
            when day_past_due_product between 91 and 120 then 5
            when day_past_due_product between 121 and 150 then 6
            when day_past_due_product between 151 and 180 then 7
            when day_past_due_product > 180 then 8
            else 0 end
    ,case when product_type in ('SPL', 'BCL', 'SCL') and day_past_due > 120 then 1
          when product_type not in ('SPL', 'BCL', 'SCL') and day_past_due > 180 then 1
          else 0 end
    ,pt_date
)

insert overwrite table id_finpro.flow_rate_base_all_new
select 
     product_code
    ,product_type
    ,loan_maturity_date
    ,loan_disbursement_date
    ,tenor
    ,tenor_in_month
    ,payment_freq
    ,int_real_rate
    ,source
    ,day_past_due
    ,day_past_due_client
    ,day_past_due_product
    ,bucket_loan
    ,bucket_client
    ,bucket_product
    ,wo_flag
    ,cur_balance
    ,os_new_disburse
    ,existing_balance
    ,wo_principal
    ,wo_interest
    ,clawback_principal
    ,clawback_interest
    ,sum(wo_principal) over (partition by substring(pt_date, 1, 7), product_code,product_type,loan_maturity_date,loan_disbursement_date,tenor,tenor_in_month,payment_freq,int_real_rate,source,day_past_due,day_past_due_client,day_past_due_product,bucket_loan,bucket_client,bucket_product,wo_flag order by pt_date rows unbounded preceding) as wo_principal_mtd
    ,sum(wo_interest) over (partition by substring(pt_date, 1, 7), product_code,product_type,loan_maturity_date,loan_disbursement_date,tenor,tenor_in_month,payment_freq,int_real_rate,source,day_past_due,day_past_due_client,day_past_due_product,bucket_loan,bucket_client,bucket_product,wo_flag order by pt_date rows unbounded preceding) as wo_interest_mtd
    ,sum(clawback_principal) over (partition by substring(pt_date, 1, 7), product_code,product_type,loan_maturity_date,loan_disbursement_date,tenor,tenor_in_month,payment_freq,int_real_rate,source,day_past_due,day_past_due_client,day_past_due_product,bucket_loan,bucket_client,bucket_product,wo_flag order by pt_date rows unbounded preceding) as clawback_principal_mtd
    ,sum(clawback_interest) over (partition by substring(pt_date, 1, 7), product_code,product_type,loan_maturity_date,loan_disbursement_date,tenor,tenor_in_month,payment_freq,int_real_rate,source,day_past_due,day_past_due_client,day_past_due_product,bucket_loan,bucket_client,bucket_product,wo_flag order by pt_date rows unbounded preceding) as clawback_interest_mtd
    ,pt_date
from prefinal_base;