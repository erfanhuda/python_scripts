set spark.sql.hive.convertMetastoreOrc=true;
with mk_wo_base as (
    select A.product_code, A.tenor, A.tenor_in_month, A.payment_freq, A.int_real_rate, A.loan_disbursement_date, A.write_off_principal, A.clawback_interest, A.clawback_principal
    , B.pt_date
    ,max(B.day_past_due) over (partition by B.pt_date,B.client_no) as day_past_due_client
    ,max(B.day_past_due) over (partition by B.pt_date,B.group_client_no) as day_past_due_group_client
    ,max(B.day_past_due) over (partition by B.pt_date,B.client_no,B.product_type) as day_past_due_product
    from dm.rep_fin_reg_db_master_kredit_write_off_ss_d A
    left join dm.rep_fin_reg_com_master_kredit_ss_d B
    on B.pt_date = add_months(A.written_off_date,-1) and A.loan_no = B.loan_no
    where (A.written_off_date = A.pt_date) or (A.clawback_date = A.pt_date);
)
, mk_wo as (
    select 
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
    tenor,
    nvl(tenor_in_month,tenor) as tenor_in_month,
    nvl(payment_freq,1) as payment_freq,
    int_real_rate,
    loan_disbursement_date,
    sum(case when written_off_date = pt_date then write_off_principal
         else 0 end) wo_principal,
    sum(case when written_off_date = pt_date then write_off_interest
         else 0 end) wo_interest,
    sum(case when clawback_date = pt_date then clawback_interest
         else 0 end) clawback_interest,
    sum(case when clawback_date = pt_date then clawback_principal
         else 0 end) clawback_principal,
    pt_date
    from mk_wo_base
    where 
    pt_date between '{{start_date}}' and '{{end_date}}'
    -- pt_date = date_add(current_date(), -1)
    group by 
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
    end,
    int_real_rate,
    loan_disbursement_date,
    tenor,
    tenor_in_month,
    payment_freq,
    pt_Date
)

insert overwrite table id_finpro.flow_rate_base_wo partition (pt_date)
select 
    product_type ,
    tenor,
    tenor_in_month,
    payment_freq,
    int_real_rate,
    loan_disbursement_date,
    wo_principal,
    wo_interest,
    clawback_principal,
    clawback_interest,
    sum(wo_principal) over (partition by substring(pt_date,1,7), tenor, tenor_in_month, payment_freq, int_real_rate,loan_disbursement_date order by pt_date rows unbounded preceding) as wo_principal_mtd,
    sum(wo_interest) over (partition by substring(pt_date,1,7), tenor, tenor_in_month, payment_freq, int_real_rate,loan_disbursement_date order by pt_date rows unbounded preceding) as wo_interest_mtd,
    sum(clawback_principal) over (partition by substring(pt_date,1,7), tenor, tenor_in_month, payment_freq, int_real_rate,loan_disbursement_date order by pt_date rows unbounded preceding) as clawback_principal_mtd,
    sum(clawback_interest) over (partition by substring(pt_date,1,7), tenor, tenor_in_month, payment_freq, int_real_rate,loan_disbursement_date order by pt_date rows unbounded preceding) as clawback_interest_mtd,
    pt_date
from mk_wo