with series as (
    select last_day(add_months(last_day(date_format('2020-06-01', 'yyyy-MM')), a.pos)) pt_date from
    (select posexplode(split(repeat("o", months_between(last_day(date_add(current_date,-1)), '2020-06-01')), "o"))) A
)
, date_series as (
    select base.pt_date as base_date, trace.pt_date as trace_date ,months_between(trace.pt_date, base.pt_date) as period
    from series base, series trace
    where base.pt_date < trace.pt_date
)
, client_info as (
    select created_date, last_day(created_date) last_created_date, cust_id, group_client_no, client_no
    from dm.rep_fin_reg_master_combined_cif_ss_d
    where pt_Date = date_add(current_date, -1)
)
, client_population as (
    select B.pt_date
        , A.created_date as origin_date
        , A.last_created_date as last_origin_date
        , min(B.pt_date) over (partition by B.cust_id) as first_date
        , B.cust_id
        , B.seg_obs
    , case
        when B.score is null then "00. Null"
        when B.score = 0  then "B1.0dpd 0"
        when B.seg_obs = "0dpd" and B.score <= 400 then "B2.0dpd 1-400"
        when B.seg_obs = "0dpd" and B.score <= 450 then "B3.0dpd 400-450"
        when B.seg_obs = "0dpd" and B.score <= 500 then "B4.0dpd 450-500"
        when B.seg_obs = "0dpd" and B.score <= 540 then "B5.0dpd 500-540"
        when B.seg_obs = "0dpd" and B.score <= 580 then "B6.0dpd 540-580"
        when B.seg_obs = "0dpd" and B.score <= 620 then "B7.0dpd 580-620"
        when B.seg_obs = "0dpd" and B.score <= 660 then "B8.0dpd 620-660"
        when B.seg_obs = "0dpd" and B.score <= 700 then "B9.0dpd 660-700"
        when B.seg_obs = "0dpd" and B.score <= 1000 then "B10.0dpd 700-1000"
        when B.seg_obs in ("xdpd","30dpd") and B.score <= 200 then "B11.xdpd 1-200"
        when B.seg_obs in ("xdpd","30dpd") and B.score <= 220 then "B12.xdpd 200-220"
        when B.seg_obs in ("xdpd","30dpd") and B.score <= 260 then "B13.xdpd 220-260"
        when B.seg_obs in ("xdpd","30dpd") and B.score <= 300 then "B14.xdpd 260-300"
        when B.seg_obs in ("xdpd","30dpd") and B.score <= 340 then "B15.xdpd 300-340"
        when B.seg_obs in ("xdpd","30dpd") and B.score <= 380 then "B16.xdpd 340-380"
        when B.seg_obs in ("xdpd","30dpd") and B.score <= 1000 then "B17.xdpd 380-1000"        
        else "B18.else" end as score_band,
    B.probability,
    B.calibrated_probability,
    B.model_feature,
    B.woe_feature,
    B.module_logodds
    from dm.credit_risk_b_score_result_ss_m B 
    left join client_info A
    on A.client_no = B.cust_id
)
, vintage_population as (
    select * from client_population 
    where pt_date >= first_date 
    group by first_date, seg_obs, score_band
)
, final as (select 
    base.last_created_date as origin_date
    , base.pt_date
    , trace.pt_date
    , base.cust_id
    , trace.cust_id
    , base.score
    , trace.score
    , base.score_band
    , trace.score_band
from client_population base, client_population trace
where base.cust_id = trace.cust_id 
and base.last_created_date < trace.last_created_date and base.pt_date < trace.pt_date)

select * from client_population;