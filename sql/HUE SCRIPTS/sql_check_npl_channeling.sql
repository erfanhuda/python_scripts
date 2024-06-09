with loan_npl as (
    select pt_date,
        product_type,
        tenor_in_month,
        ecl_bucket_max,
        loan_no,
        outstanding_contractual,
        fac_amount,
        total_int_accrued,
        total_int_accrued_adj
    from dm.rep_fin_reg_com_master_kredit_ss_d
    where pt_date = "${eom_date}"
        and product_code in (
            106,
            107,
            108,
            109,
            110,
            111,
            112,
            115,
            'C01',
            'SC1',
            'SC2',
            'M05'
        )
        and ecl_bucket_max in (5)
),
tracker as (
    select b.pt_date,
        b.product_type,
        b.tenor_in_month,
        b.ecl_bucket_max,
        count(b.loan_no) as count_loan,
        sum(b.outstanding_contractual) as outstanding,
        sum(b.fac_amount) as fac_amount,
        sum(b.total_int_accrued) as int_accrued,
        sum(b.total_int_accrued_adj) as int_accrued_adj
    from loan_npl a
        left join dm.rep_fin_reg_com_master_kredit_ss_d b on a.loan_no = b.loan_no
    where b.pt_date = "${tracked_date}"
    group by b.pt_date,
        b.product_type,
        b.tenor_in_month,
        b.ecl_bucket_max
)
select pt_date,
    product_type,
    tenor_in_month,
    ecl_bucket_max,
    count(loan_no) as count_loan,
    sum(outstanding_contractual) as outstanding,
    sum(fac_amount) as fac_amount,
    sum(total_int_accrued) as int_accrued,
    sum(total_int_accrued_adj) as int_accrued_adj
from loan_npl
group by pt_date,
    product_type,
    tenor_in_month,
    ecl_bucket_max
union all
select *
from tracker;