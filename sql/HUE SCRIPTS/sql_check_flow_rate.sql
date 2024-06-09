-- Cek Data Amount Flow Rate MK Active
select pt_date,
    product_type,
    case
        when day_past_due_client = 0 then '1.Current'
        when day_past_due_client between 1 and 30 then '2.1-30DPD'
        when day_past_due_client between 31 and 60 then '3.31-60DPD'
        when day_past_due_client between 61 and 90 then '4.61-90DPD'
        when day_past_due_client between 91 and 120 then '5.91-120DPD'
    end as bucket_client,
    sum(cur_balance) as cur_balance
from id_finpro.flow_rate_base_active
group by pt_date,
    product_type,
    case
        when day_past_due_client = 0 then '1.Current'
        when day_past_due_client between 1 and 30 then '2.1-30DPD'
        when day_past_due_client between 31 and 60 then '3.31-60DPD'
        when day_past_due_client between 61 and 90 then '4.61-90DPD'
        when day_past_due_client between 91 and 120 then '5.91-120DPD'
    end;
-- Cek Data Amount Flow Rate MK WO
select pt_date,
    product_type,
    tenor,
    tenor_in_month,
    payment_freq,
    sum(wo_principal) as wo_principal,
    sum(wo_interest) as wo_interest,
    sum(clawback_principal) as clawback_principal,
    sum(clawback_interest) as clawback_interest
from id_finpro.flow_rate_base_wo
where pt_date > "2022-12-31"
group by pt_date,
    product_type,
    tenor,
    tenor_in_month,
    payment_freq;
select distinct pt_date
from id_finpro.flow_rate_base_active;