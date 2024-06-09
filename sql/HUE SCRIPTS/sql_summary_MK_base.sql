with active_population as (
    select a.pt_Date,
        a.ec_number,
        a.import_source,
        a.product_type,
        a.tenor,
        "" as write_off_date,
        "" as settle_date,
        count(a.loan_no) as loan_no,
        count(distinct a.client_no) as client_no,
        sum(a.cur_balance) as cur_balance
    From dm.rep_fin_reg_com_master_kredit_ss_m a
    where a.pt_date <= '${pt_date}'
        and a.ec_number in ('EC1')
    group by a.pt_Date,
        a.ec_number,
        a.import_source,
        a.product_type,
        a.tenor
),
wo_population as(
    select a.pt_Date,
        a.ec_number,
        a.import_source,
        a.product_type,
        a.tenor,
        a.written_off_date,
        "" as settle_date,
        count(a.loan_no) as loan_no,
        count(distinct a.client_no) as client_no,
        sum(a.write_off_principal) as cur_balance
    From dm.rep_fin_reg_db_master_kredit_write_off_ss_d a
    where a.pt_date = '${pt_date}'
    group by a.pt_Date,
        a.ec_number,
        a.import_source,
        a.product_type,
        a.tenor,
        a.written_off_date
),
close_population as (
    select a.pt_Date,
        a.ec_number,
        a.import_source,
        a.product_type,
        a.tenor,
        "" as write_off_date,
        a.settle_date,
        count(a.loan_no) as loan_no,
        count(distinct a.client_no) as client_no,
        sum(a.cur_balance) as cur_balance
    From dm.rep_fin_reg_db_master_kredit_settled_ss_m a
    where a.pt_date = "${pt_date}"
        and a.ec_number in ('EC2', 'EC3')
    group by a.pt_Date,
        a.ec_number,
        a.import_source,
        a.product_type,
        a.tenor,
        a.settle_date
),
final as (
    select *
    from active_population
    union all
    select *
    from wo_population
    union all
    select *
    from close_population
)
select count(*)
from final;