with raw_data as (
	select 
	pt_date,
	account_no,
	client_no,
	pd_segment,
	loan_maturity_date,
	payment_date,
	bakidebet,
	bmhd_final,
	kol_cif_final,
	dpd_acct,
	restructure,
	sicr_override,
	revolving_flag,
	ecl_bucket_acct,
	ecl_bucket_cif,
	stage_acct,
	stage_cif,
	int_real_rate
	, gca
	, ead_drawn
	, ead_undrawn
	, ead_lifetime
	, ead_12m
	, lgd
	, indiv_collective
	, sum(pd_base) as cpd_base
	, sum(pd_best) as cpd_best
	, sum(pd_worst) as cpd_worst
	, sum(pd_weight) as cpd_weight
	, sum(principal_payment) as prin_payment
	, sum(unamortized_cost_fee) as unamortized_cost_fee
	, sum(ecl_final_base) as ecl_final_base
	, sum(ecl_final_best) as ecl_final_best
	, sum(ecl_final_worst) as ecl_final_worst
	, sum(ecl_final_weighted) as ecl_final_weighted
	from public.bv_ecl_process_det13 A, public.bv_ecl_process_det13 B
	on A.account_no = B.account_no
	group by pt_date,account_no,client_no,pd_segment,loan_maturity_date,payment_date,bakidebet,bmhd_final,kol_cif_final,dpd_acct,restructure,sicr_override,revolving_flag,ecl_bucket_acct,ecl_bucket_cif,stage_acct,stage_cif,int_real_rate, gca, ead_drawn, ead_undrawn, ead_lifetime, ead_12m, lgd, indiv_collective
)

select * from raw_data;