select mom_date,
    pd_segment,
    tenor,
    settle_type,
    ecl_bucket_client,
    ecl_bucket_client_mom,
    default_flag_client,
    count_of_loans,
    count_of_clients
from rep_fin_ecl_pd_data_agg_mom_ss_m
where mom_date < last_day('${query_date}');
select qoq_date,
    pd_segment,
    tenor,
    settle_type,
    ecl_bucket_client,
    ecl_bucket_client_qoq,
    default_flag_client,
    count_of_loans,
    count_of_clients
from rep_fin_ecl_pd_data_agg_qoq_ss_m
where qoq_date < last_day('${query_date}');
select hoh_date,
    pd_segment,
    tenor,
    settle_type,
    ecl_bucket_client,
    ecl_bucket_client_hoh,
    default_flag_client,
    count_of_loans,
    count_of_clients
from rep_fin_ecl_pd_data_agg_hoh_ss_m
where hoh_date < last_day('${query_date}');
select yoy_date,
    pd_segment,
    tenor,
    settle_type,
    ecl_bucket_client,
    ecl_bucket_client_yoy,
    default_flag_client,
    count_of_loans,
    count_of_clients
from rep_fin_ecl_pd_data_agg_yoy_ss_m
where yoy_date < last_day('${query_date}');
select *
from rep_fin_ecl_pd_ttc_pd_qoq_ss_m;
select *
from rep_fin_ecl_pd_ttc_pd_mom_ss_m;
select *
from rep_fin_ecl_pd_ttc_pd_hoh_ss_m;
select *
from rep_fin_ecl_pd_ttc_pd_yoy_ss_m;
select *
from rep_fin_ecl_pd_data_agg_mom_ss_m
select *
from rep_fin_ecl_pd_data_agg_qoq_ss_m
select *
from rep_fin_ecl_pd_data_agg_hoh_ss_m
select *
from rep_fin_ecl_pd_data_agg_yoy_ss_m
select *
from rep_fin_ecl_pd_pit_pd_mom_ss_m
select *
from rep_fin_ecl_pd_pit_pd_qoq_ss_m
select *
from rep_fin_ecl_pd_pit_pd_hoh_ss_m
select *
from rep_fin_ecl_pd_pit_pd_yoy_ss_m
select *
from rep_fin_ecl_pd_pit_pd_mom_ss_m;
select *
from rep_fin_ecl_pd_pit_pd_qoq_ss_m;
select *
from rep_fin_ecl_pd_pit_pd_hoh_ss_m;
select *
from rep_fin_ecl_pd_pit_pd_yoy_ss_m;
select pd_segment,
    tenor,
    qoq_date,
    pt_date,
    avg(odr_loan)
from ecl.rep_fin_ecl_pd_odr_qoq_ss_m
where qoq_date between "2022-10-31" and "2023-06-30"
group by pd_segment,
    tenor,
    qoq_date,
    pt_date;