with active_population as (
    select a.pt_Date,
        a.ec_number,
        a.import_source,
        a.product_type,
        a.tenor,
        a.write_off_date,
        a.settle_date,
        count(a.loan_no) as loan_no,
        count(distinct a.client_no) as client_no
    From ecl.rep_fin_ecl_prep_master_data_ss_m a
    where a.pt_date <= '${pt_date}'
        and a.ec_number in ('EC1')
    group by a.pt_Date,
        a.ec_number,
        a.import_source,
        a.loan_no,
        a.product_type,
        a.tenor,
        a.write_off_date,
        a.settle_date
),
wo_close_population as (
    select a.pt_Date,
        a.ec_number,
        a.import_source,
        a.product_type,
        a.tenor,
        a.write_off_date,
        a.settle_date,
        count(a.loan_no) as loan_no,
        count(distinct a.client_no) as client_no
    From ecl.rep_fin_ecl_prep_master_data_ss_m a
    where a.pt_date = '${pt_date}'
        and a.ec_number in ('EC2', 'EC3')
    group by a.pt_Date,
        a.ec_number,
        a.import_source,
        a.loan_no,
        a.product_type,
        a.tenor,
        a.write_off_date,
        a.settle_date
),
final as (
    select *
    from active_population
    union all
    select *
    from wo_close_population
)
select count(*)
from final;