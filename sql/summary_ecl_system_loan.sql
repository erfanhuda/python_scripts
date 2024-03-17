select 
  b.pt_date
  , b.tenor
  , b.ecl_bucket_client
  , b.product_code
  , b.period
  , b.int_real_rate
  , b.pd_base
  , b.lgd
  , b.discount_rate
  , a.loan_disbursement_date
  , count(b.loan_no) as count_loan
  , sum(b.cur_balance) as cur_balance
  , sum(b.accrued_interest_pl) as accrued_interest_pl
  , sum(b.ead_drawn) as ead_drawn
  , sum(b.ead_undrawn) as ead_undrawn
  , sum(b.ecl_final) as ecl_final
  , sum(b.ecl_drawn_weighted) as ecl_drawn
  , sum(b.ecl_undrawn_weighted) as lifetime_ecl_undrawn
from ecl.rep_fin_ecl_ecl_term_structure_ss_m b
left join dm.rep_fin_reg_com_master_kredit_ss_d a
on b.pt_date = a.pt_date and a.loan_no = b.loan_no
where b.pt_date = '{{pt_date}}' and b.period = 1 and b.product_code = 103 and b.tenor = 3
group by b.pt_date
  , b.tenor
  , b.ecl_bucket_client
  , b.product_code
  , b.period
  , b.int_real_rate
  , b.pd_base
  , b.lgd
  , b.discount_rate
  , a.loan_disbursement_date;