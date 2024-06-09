select product_code,
    debtor_ctgy,
    product_type,
    product_category,
    product_code,
    product_code_desc,
    segment_1,
    segment_2,
    int_real_rate,
    count(loan_no) as count_loan,
    count(distinct client_no) as count_client
from dm.rep_fin_reg_com_master_kredit_ss_d
where pt_date = "${date}"
group by product_code,
    debtor_ctgy,
    product_type,
    product_category,
    product_code,
    product_code_desc,
    segment_1,
    segment_2,
    int_real_rate;