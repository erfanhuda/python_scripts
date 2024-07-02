set spark.sql.hive.convertMetastoreOrc=true;
with table_wo as (
    select 
        written_off_date,
        loan_no,
        client_no,
        group_client_no,
        product_code,
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
        write_off_principal,
        write_off_interest,
        clawback_principal,
        clawback_interest , 
        date_sub(written_off_date,1) as wo_date_min1
    from dm.rep_fin_reg_db_master_kredit_write_off_ss_d 
    where pt_date = written_off_date
    and pt_date between '{{start_date}}' and '{{end_date}}'
    -- and loan_no in ('1046120495020331057757860', '1056074547753586712677346', '1055903638359060505597997')
)
, table_mk_com as (
    select pt_date, loan_no, day_past_due, cur_balance
    from dm.rep_fin_reg_com_master_kredit_ss_d
    where pt_date between date_sub('{{start_date}}',1) and '{{end_date}}'
)
insert overwrite table id_finpro.master_base_wo_120 partition (pt_date) 
select 
    a.loan_no
    , a.client_no
    , a.group_client_no
    , a.product_code
    , a.product_type
    , a.int_real_rate
    , a.tenor
    , a.tenor_in_month
    , a.payment_freq 
    , b.day_past_due 
    , b.cur_balance 
    , a.loan_disbursement_date
    , a.write_off_principal
    , a.write_off_interest
    , a.clawback_principal
    , a.clawback_interest
    , b.pt_date as wo_date_min1
    , a.written_off_date as pt_date
from table_wo as a 
left join table_mk_com as b on (a.loan_no = b.loan_no and a.wo_date_min1 = b.pt_date)