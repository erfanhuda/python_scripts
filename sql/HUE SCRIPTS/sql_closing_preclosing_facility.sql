select parent_product_id,
    product_type,
    status,
    col,
    int_rate,
    last_day(facility_start_date) as facility_start_date,
    count(facility_no) as count_facility,
    sum(count_loan) as count_loan,
    sum(available_limit) as available_limit,
    sum(total_limit) as total_limit
from dm.rep_fin_reg_com_master_facility_ss_d
where pt_date = '${date}'
    and status = 'EFFECTIVE'
group by parent_product_id,
    product_type,
    status,
    col,
    int_rate,
    last_day(facility_start_date);
with open_date as (
    select client_no,
        max(
            from_unixtime(
                unix_timestamp(accounting_date, 'yyyyMMdd'),
                'yyyy-MM-dd'
            )
        ) as accounting_date
    from ods.cbs_id_bke_loan_core_db_credit_limit_status_flow_tab_ss
    where pt_Date = '${date}'
        and prod_Type = '812'
        and tran_type = 'OPEN'
    group by client_no
),
limit_id as (
    select a.client_no,
        a.limit_id,
        b.accounting_date,
        a.accounting_date as acct_date
    from ods.cbs_id_bke_loan_core_db_credit_limit_status_flow_tab_ss a
        inner join open_date b on a.client_no = b.client_no
    where a.pt_Date = '${date}'
        and prod_Type = '812'
        and tran_type = 'OPEN'
        and from_unixtime(
            unix_timestamp(a.accounting_date, 'yyyyMMdd'),
            'yyyy-MM-dd'
        ) = b.accounting_date
),
total_limit as (
    select b.client_no,
        b.limit_id,
        b.accounting_date,
        a.total_limit
    from ods.cbs_id_bke_loan_core_db_credit_limit_tab_ss a
        inner join limit_id b on a.client_no = b.client_no
        and a.unique_id = b.limit_id
        and a.accounting_date = b.acct_date
    where a.pt_date = '${date}'
        and a.prod_type = '812'
),
curr_MF as (
    select case
            when a.facility_no is null then b.limit_id
            else a.facility_no
        end as facility_no,
        a.client_no,
        a.branch,
        a.loan_type,
        a.product_type,
        a.parent_product_id,
        a.available_limit,
        a.unused_uncomm,
        a.unused_comm,
        case
            when a.facility_no is null then b.total_limit
            else a.total_limit
        end as total_limit,
        case
            when a.facility_no is null then case
                when a.status = 'EFFECTIVE' then b.total_limit
                else a.cont_amt
            end
            else a.eff_total_limit
        end as eff_total_limit,
        a.status,
        a.count_loan,
        nvl(a.facility_start_date, b.accounting_date) as facility_start_date,
        add_months(
            nvl(a.facility_start_date, b.accounting_date),
            36
        ) as facility_end_date,
        a.currency,
        a.city_code,
        a.int_type,
        a.int_rate,
        a.cont_amt,
        a.loan_amt,
        a.loan_disb,
        a.int_accrue,
        a.col,
        a.debt_cat,
        a.port_cat,
        a.loan_char,
        a.loan_purp,
        a.loan_orien,
        a.loan_govpro,
        a.loan_sector,
        a.pk_grouping_id,
        a.ec_number,
        a.city_code_lokasi,
        a.contract_name,
        a.contract_number,
        a.uid,
        a.import_source,
        a.pt_date
    from dm.rep_fin_reg_com_master_facility_ss_d a
        left join total_limit b on a.client_no = b.client_no
    where a.pt_date = last_day('${date}')
)
select pt_date,
    parent_product_id,
    product_type,
    status,
    col,
    int_rate,
    last_day(facility_start_date) as facility_start_date,
    last_day(facility_end_date) as facility_end_date,
    count(*) as count_facility,
    sum(count_loan) as count_loan,
    sum(available_limit) as available_limit,
    sum(total_limit) as total_limit
from curr_MF
where status = 'EFFECTIVE'
group by pt_Date,
    parent_product_id,
    product_type,
    status,
    col,
    int_rate,
    last_day(facility_start_date),
    last_day(facility_end_date);