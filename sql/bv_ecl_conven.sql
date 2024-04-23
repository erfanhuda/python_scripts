with raw_data as (
	select 
		a.pt_date,
		a.account_no,
		a.client_no,
		a.pd_segment,
		a.loan_maturity_date,
		a.payment_date,
		a.bakidebet,
		a.bmhd_final,
		a.kol_cif_final,
		a.dpd_acct,
		a.restructure,
		a.sicr_override,
		a.revolving_flag,
		a.ecl_bucket_acct,
		a.ecl_bucket_cif,
		a.stage_acct,
		a.stage_cif,
		a.int_real_rate
		, a.gca
		, a.ead_drawn
		, a.ead_undrawn
		, a.ead_lifetime
		, a.ead_12m
		, a.lgd
		, a.indiv_collective
		, sum(a.pd_base) as cpd_base
		, sum(a.pd_best) as cpd_best
		, sum(a.pd_worst) as cpd_worst
		, sum(a.pd_weight) as cpd_weight
		, sum(a.principal_payment) as prin_payment
		, sum(a.unamortized_cost_fee) as unamortized_cost_fee
		, sum(a.ecl_final_base) as ecl_final_base
		, sum(a.ecl_final_best) as ecl_final_best
		, sum(a.ecl_final_worst) as ecl_final_worst
		, sum(a.ecl_final_weighted) as ecl_final_weighted
		from public.bv_ecl_process_det13 A, public.bv_ecl_process_det13 B
		on A.account_no = B.account_no
		group by a.pt_date,a.account_no,a.client_no,a.pd_segment,a.loan_maturity_date,a.payment_date,a.bakidebet,a.bmhd_final,a.kol_cif_final,a.dpd_acct,a.restructure,a.sicr_override,a.revolving_flag,a.ecl_bucket_acct,a.ecl_bucket_cif,a.stage_acct,a.stage_cif,a.int_real_rate, a.gca, a.ead_drawn, a.ead_undrawn, a.ead_lifetime, a.ead_12m, a.lgd, a.indiv_collective
)

select * from raw_data;