--说明：1. 本PRD中，所有'{{dt}}'分区均配置为上个月月末最后一天last_day(add_months('{{dt}}',-1))  ==> 改动1

-- step1. create a validation result report for those 'not passed'
create table rr_test.rep_fin_ecl_prep_base_default_flag_exception_report_ss_m
(xxx)
partitioned by (pt_date string);
;

insert overwrite table rr_test.rep_fin_ecl_prep_base_default_flag_exception_report_ss_m
partition (pt_date) 
select loan_no, client_no, day_past_due_acct, day_past_due_client, ecl_bucket_acct, ecl_bucket_client, default_flag_acct, default_flag_client,default_flag_exception,pt_date
from (
select loan_no, client_no, day_past_due_acct, day_past_due_client, ecl_bucket_acct, ecl_bucket_client, default_flag_acct, default_flag_client,
case
when (day_past_due_acct = 0 and ecl_bucket_acct <> 1) 
or (day_past_due_acct between 1 and 30 and ecl_bucket_acct <> 2)
or (day_past_due_acct between 31 and 60 and ecl_bucket_acct <> 3)
or (day_past_due_acct between 61 and 90 and ecl_bucket_acct <> 4)
or (day_past_due_acct > 90 and ecl_bucket_acct <> 5)
or (day_past_due_client = 0 and ecl_bucket_client <> 1)
or (day_past_due_client between 1 and 30 and ecl_bucket_client <> 2)
or (day_past_due_client between 31 and 60 and ecl_bucket_client <> 3)
or (day_past_due_client between 61 and 90 and ecl_bucket_client <> 4)
or (day_past_due_client > 90 and ecl_bucket_client <> 5)
then "NOT PASSED"
else "PASSED"
end as default_flag_exception,
'{{dt}}' as pt_date
from ecl.rep_fin_ecl_prep_master_data_ss_m
where pt_date=last_day('{{dt}}')
)t
where t.default_flag_exception= "NOT PASSED";

---new enhancement in ecl.prep_base  ---removed 2024.02.29 as too effort to rerun historical prep_base data
---create table if not exists ecl.rep_fin_ecl_prep_base_data_ss_m (
--  xxxx
--  ,accounting_status string)
--
--insert overwrite table ecl.rep_fin_ecl_prep_base_data_ss_m partition (pt_date = '{dt}')
--select
--xxx
--,'MK_OUTSTANDING'
--,'EC1'
--,MASTER_KREDIT.accounting_status
--
--union all
--xxx
--,'MK_WRITEOFF'
--,'EC2'
--,'WRITE_OFF'
--
--union all
--xxxx
--,'MK_SETTLED'
--,'EC3'
--,'NORMAL'

-- step2. create base_population_cohort_track at Mt with table name as 'base_population_cohort_track'

---to discuss with erfan ????2024.02.29 but we do need add some extra condition with the first default date and last default date
--(1) M0, if the last default date < pt_date and bucket in 1,2,3,4 then it equal to bucket, if not it equal to 5, 
--(2) and in MT movement if the loan of m0date are not default and the last default date between m0date and pt_date of MT it should be ever default

--ddl
create table ecl.rep_fin_ecl_pd_cohort_base_t_ss_m
(m0_date string  
,max_period         bigint
,max_period_default bigint
,loan_no        string
,client_no      string
,prod_code   	string
,prod_desc      string
,tenor          string
,collectability  string
,pd_cohort_first_default_date		string
,pd_cohort_default_flag 			string
,pd_cohort_ecl_bucket         string  
,write_off_date  	string
,int_real_rate		decimal(36,10)
,src_ec				string 
,condition			string
,wo_flag		string
,ecl_bucket     string
,default_flag   string            
,cur_balance   decimal(36,10)
,import_source      string
,ec_number          string)
partitioned by (pt_date string);

with default_info as 
(select loan_no
,client_no
,last_day(min(accounting_date)) as first_default_date    ---- business meaning: cohort_date  (seems the similar to mk_daily.first_default_date)
from dwd.t05_pl_loan_accounting_status_change_evt_ss_d 
where pt_date= last_day('{{dt}}')   
and accounting_status_code ='NON_PERFORMING'
and accounting_date <= last_day('{{dt}}')   
group by loan_no,client_no) 

,default_info_cif as 
(select client_no
,min(default_info.first_default_date) as first_default_date_client
from default_info
group by client_no)

,cur_acct_status as (   ---2024.02.29
select pre_base.loan_no, pre_base.client_no, case when ec_number = 'EC2' then 'WRITE_OFF' 
                        when ec_number = 'EC3' then 'NORMAL' 
                        else MASTER_KREDIT.accounting_status end as accounting_status 
from ecl.rep_fin_ecl_prep_base_data_ss_m pre_base 
left join ( select accounting_status, loan_no, client_no
                from dm.rep_fin_reg_com_master_kredit_ss_d 
                where pt_date = last_day('{{dt}}')   
                ) MASTER_KREDIT 
                on pre_base.loan_no = MASTER_KREDIT.loan_no 
                where pre_base.pt_date= last_day('{{dt}}') )

---2024.02.26 new to compute the worst default flag per client_no
,default_flag_client as
(select client_no
,case when sum(case when accounting_status in ('NON_PERFORMING','WRITE_OFF') then 1 else 0 end)>0 then 'Y' else 'N' end  as default_flag_client
from cur_acct_status  ---2024.02.29
group by client_no)


insert overwrite table ecl.rep_fin_ecl_pd_cohort_base_t_ss_m partition (pt_date) 

select
last_day(prep_base.loan_disbursement_date) 												as m0_date 
,nvl(months_between(pt_date,last_day(prep_base.loan_disbursement_date))+1,0)		as max_period 
,case when default_info.first_default_date is null then 0
else months_between(pt_date,default_info.first_default_date)+1	end			as max_period_default ---- business meaning: cohort_index
,prep_base.loan_no																		as loan_no
,prep_base.client_no																	as client_no
,prep_base.product_code																	as prod_code
,prep_base.product_type																	as prod_desc
,prep_base.tenor																		as tenor
,prep_base.collectability_acct															as collectability
,nvl(default_info.first_default_date,'')												as pd_cohort_first_default_date
,nvl(case when default_info.first_default_date is not null then 'Y' end,'N')			as pd_cohort_default_flag 
,CASE when default_info.first_default_date is null then prep_base.ecl_bucket_acct
--    WHEN datediff('{{dt}}',nvl(default_info.first_default_date,''))=0 THEN 1
--    WHEN datediff('{{dt}}',nvl(default_info.first_default_date,''))> 0 and  datediff('{{dt}}',nvl(default_info.first_default_date,'')) <=30  THEN 2
--    WHEN datediff('{{dt}}',nvl(default_info.first_default_date,'')) > 30 and datediff('{{dt}}',nvl(default_info.first_default_date,'')) <=60 THEN 3
--    WHEN datediff('{{dt}}',nvl(default_info.first_default_date,'')) > 60 and datediff('{{dt}}',nvl(default_info.first_default_date,'')) <=90 THEN 4
    ELSE 5 END                                          as pd_cohort_ecl_bucket       ---2024.03.05 npl status means dpd >=91
,prep_base.write_off_date																as write_off_date  
,prep_base.int_real_rate																as int_real_rate
,concat(prep_base.IMPORT_SOURCE,"_", prep_base.ec_number)								as src_ec 
--- assumption in prep_base: ecl_bucket default to 5 and dpd default to 180 for wo (EC2); 
-- ecl_bucket default to 1 and dpd default to 0 for settled (E3) loans 
,CASE WHEN PREP_BASE.EC_NUMBER = 'EC1' and PREP_BASE.ECL_BUCKET_ACCT <>'5' and PREP_BASE.DAY_PAST_DUE_ACCT<'90' and default_info.first_default_date is null THEN '0_Active'
WHEN PREP_BASE.EC_NUMBER = 'EC3' THEN '2_Settled'   --- not expect any value as no more settled loan
WHEN PREP_BASE.EC_NUMBER = 'EC2' and default_info.first_default_date is not null THEN '3_WO'
else '1_Default' end 																	as condition
,prep_base.default_flag_acct 															as wo_flag
,prep_base.ecl_bucket_acct																as ecl_bucket
,case when cur_acct_status.accounting_status in ('NON_PERFORMING','WRITE_OFF') then 'Y' else 'N' end as default_flag    ---2024.02.29
,prep_base.cur_balance																	as cur_balance
,'acct level' 																			as import_source
,'EC1'																				 	as ec_number	
,'{{dt}}' as pt_date												
from ecl.rep_fin_ecl_prep_base_data_ss_m prep_base
left join default_info 
on prep_base.loan_no=default_info.loan_no
left join cur_acct_status
on prep_base.loan_no=cur_acct_status.loan_no
where prep_base.pt_date = last_day('{{dt}}')   

union all

select
last_day(prep_base.loan_disbursement_date) 												as m0_date
,nvl(months_between(pt_date,last_day(prep_base.loan_disbursement_date))+1,0)		as max_period	
,case when default_info_cif.first_default_date_client is null then 0
else months_between(pt_date,default_info_cif.first_default_date_client)+1	end		as max_period_default			
,prep_base.loan_no																		as loan_no
,prep_base.client_no																	as client_no
,prep_base.product_code																	as prod_code
,prep_base.product_type																	as prod_desc
,prep_base.tenor																		as tenor
,prep_base.collectability_client														as collectability
,nvl(default_info_cif.first_default_date_client,'')												as pd_cohort_first_default_date
,nvl(case when default_info_cif.first_default_date_client is not null then 'Y' end,'N')			as pd_cohort_default_flag
,CASE when default_info_cif.first_default_date_client is null then prep_base.ecl_bucket_client
--    WHEN datediff('{{dt}}',nvl(default_info_cif.first_default_date_client,''))=0 THEN 1
--    WHEN datediff('{{dt}}',nvl(default_info_cif.first_default_date_client,'')) > 0 and  datediff('{{dt}}',nvl(default_info_cif.first_default_date_client,'')) <=30  THEN 2
--    WHEN datediff('{{dt}}',nvl(default_info_cif.first_default_date_client,'')) > 30 and datediff('{{dt}}',nvl(default_info_cif.first_default_date_client,'')) <=60 THEN 3
--    WHEN datediff('{{dt}}',nvl(default_info_cif.first_default_date_client,'')) > 60 and datediff('{{dt}}',nvl(default_info_cif.first_default_date_client,'')) <=90 THEN 4
    ELSE 5 END                                           as pd_cohort_ecl_bucket      --2024.03.05
,prep_base.write_off_date																as write_off_date  
,prep_base.int_real_rate																as int_real_rate
,concat(prep_base.IMPORT_SOURCE,"_", prep_base.ec_number)								as src_ec   
,CASE WHEN PREP_BASE.EC_NUMBER = 'EC1' and PREP_BASE.ecl_bucket_client <>'5' and PREP_BASE.day_past_due_client<'90' and default_info_cif.first_default_date_client is null THEN '0_Active'
WHEN PREP_BASE.EC_NUMBER = 'EC3' THEN '2_Settled'  
WHEN PREP_BASE.EC_NUMBER = 'EC2' and default_info_cif.first_default_date_client is not null THEN '3_WO'
else '1_Default' end 																	as condition
,prep_base.default_flag_client															as wo_flag
,prep_base.ecl_bucket_client															as ecl_bucket
,default_flag_client.default_flag_client                as default_flag   
,prep_base.cur_balance																	as cur_balance
,'client level' 																		as import_source
,'EC2'																				 	as ec_number					
,'{{dt}}' as pt_date					
from ecl.rep_fin_ecl_prep_base_data_ss_m prep_base
left join default_info_cif
on prep_base.client_no=default_info_cif.client_no
left join default_flag_client
on prep_base.client_no=default_flag_client.client_no
where prep_base.pt_date = last_day('{{dt}}')   
;

----confirmed code for Part 2 development --- starting here
--step4. Aggregated base Population accordingly -- denominator
create table ecl.rep_fin_ecl_pd_cohort_population_agg_ss_m
(prod_desc      string
,tenor          string
,pd_cohort_ecl_bucket    string      ----2024.02.28
,count_loan		bigint
,count_client	bigint
,cur_balance   decimal(36,10)
,import_source      string
,ec_number          string)
partitioned by (pt_date string);

insert overwrite table rr_test.test_rep_fin_ecl_pd_cohort_population_agg_ss_m
partition (pt_date='${dt}')    --- this is the M0 date for population
 select
                  prod_desc
                  ,tenor
                  ,pd_cohort_ecl_bucket   ------2024.03.09
                  ,count(loan_no) as count_loan
                  ,count(distinct client_no) as count_client
                  ,sum(cur_balance) as cur_balance
                  ,import_source as import_source
                  ,ec_number as ec_number
                  --,pt_date
              from ecl.rep_fin_ecl_pd_cohort_base_t_ss_m
              where condition in ('1_Default','0_Active')   ------2024.02.26 remove settled loans after user confirmation
                and import_source='client level'  ---2024.03.07
                and pt_date='${dt}'
                -- and prod_desc = 'SPL' and tenor=24.0
                --  and pt_date in ('2024-02-29','2024-01-31','2023-12-31','2023-11-30','2023-10-31','2023-09-30','2023-08-31','2023-07-31')
              group by 
                  prod_desc
                  ,tenor
                  ,pd_cohort_ecl_bucket    ------2024.03.09
                  ,import_source
                  ,ec_number
                  --,pt_date
                  ;

---create default population 
--Step3. Aggregated default Population accordingly by historical population at M0 and Mt -- numerator
--5. Cumulative/Aggregate Cohort

create table rr_test.test_rep_fin_ecl_pd_cohort_default_agg_ss_m
(period         bigint
--,pd_cohort_first_default_date    string    --- 2024.02.26 group by cohort_date 
,m0_date   string ---2024.03.06
,prod_desc      string
,tenor          string
,pd_cohort_ecl_bucket     string
,count_loan		bigint
,count_client	bigint
,cur_balance   decimal(36,10)
,import_source      string
,ec_number          string)
partitioned by (pt_date string);

With cohort_retained as 
(select 
b.pt_date as m0_date
,row_number() over (partition by b.loan_no, b.pt_date order by 1) as period
,b.loan_no
,b.client_no
,b.cur_balance
,b.prod_code
,b.prod_desc
,b.tenor
,b.int_real_rate
,b.condition
---2024.03.22
,b.pd_cohort_ecl_bucket as m0_pd_cohort_ecl_bucket
,b.ecl_bucket
,b.import_source 
,b.ec_number
,a.pt_date as mt_date 
---2024.03.22
,a.pd_cohort_ecl_bucket as pd_cohort_ecl_bucket
,a.ecl_bucket as mt_ecl_bucket
,a.pd_cohort_default_flag 
from 
    (select * from ecl.rep_fin_ecl_pd_cohort_base_t_ss_m
      where import_source='client level' and condition <> '2_Settled' and pt_date<='${dt}'
        --temp code filter for sample check
      --  and prod_desc = 'SPL' and tenor=24.0
      --  and pt_date in ('2024-02-29','2024-01-31','2023-12-31','2023-11-30','2023-10-31','2023-09-30','2023-08-31','2023-07-31')
      ) a   --- Mt observation period/tracking period 
    left join (select * from ecl.rep_fin_ecl_pd_cohort_base_t_ss_m
                  where import_source='client level' and condition in ('0_Active', '1_Default') and pt_date<='${dt}'
                  --temp code filter for sample check
         --           and prod_desc = 'SPL' and tenor=24.0
         --         	and pt_date in ('2024-02-29','2024-01-31','2023-12-31','2023-11-30','2023-10-31','2023-09-30','2023-08-31','2023-07-31')
                  ) b   ---- M0 snapshot period 
    	where a.pt_date>=b.pt_date and a.loan_no=b.loan_no)

insert overwrite table rr_test.test_rep_fin_ecl_pd_cohort_default_agg_ss_m  ----business meaning: pivot data to see cohort table
partition (pt_date='${dt}') 
              select
                  period 
                  --,pd_cohort_first_default_date --- 2024.02.26 group by cohort_date!!
                  ,m0_date  ---2024.03.06
                  ,prod_desc
                  ,tenor
                  ,pd_cohort_ecl_bucket   --- 2024.02.26
                  ,count(loan_no) as count_loan
                  ,count(distinct client_no) as count_client
                  ,sum(cur_balance) as cur_balance
                  ,import_source as import_source
                  ,ec_number as ec_number
              from cohort_retained where pd_cohort_default_flag ='Y'
              group by 
                  period
                  --,pd_cohort_first_default_date    --- 2024.02.26 group by cohort_date 
                  ,m0_date  ---2024.03.06
                  ,prod_desc
                  ,tenor
                  ,pd_cohort_ecl_bucket  --- 2024.02.26
                  ,import_source
                  ,ec_number
;


---5. marginal cohort 
--marginal cohort ddl
create table rr_test.test_rep_fin_ecl_pd_cohort_default_mrgl_ss_m
(period         bigint
,m0_date  string ---2024.03.06
,prod_desc      string
,tenor          string
,pd_cohort_ecl_bucket     string    -- 2024.02.26 enhancement
,mrgl_count_loan   bigint
,mrgl_count_client bigint
,mrgl_cur_balance   decimal(36,10)
,import_source      string
,ec_number          string)
partitioned by (pt_date string);

insert overwrite table rr_test.test_rep_fin_ecl_pd_cohort_default_mrgl_ss_m
partition (pt_date='${dt}') 
  
--2024.03.12
select 
  period
  ,m0_date 
  ,prod_desc
  ,tenor
  ,pd_cohort_ecl_bucket   --2024.02.26
  ,case when period=1 then 0
  else count_loan - lag(count_loan,1,0) over (partition by m0_date, prod_desc, tenor, pd_cohort_ecl_bucket,pt_date order by period) end as mrgl_count_loan
  ,case when period=1 then 0
  else count_client -lag(count_client,1,0) over (partition by m0_date, prod_desc, tenor, pd_cohort_ecl_bucket,pt_date order by period) end as mrgl_count_loan
  ,case when period=1 then 0
  else cur_balance - lag(cur_balance,1,0) over (partition by m0_date, prod_desc, tenor, pd_cohort_ecl_bucket,pt_date order by period) end as mrgl_cur_balance
  ,import_source
  ,ec_number
  --- 2024.03.06
from rr_test.test_rep_fin_ecl_pd_cohort_default_agg_ss_m 
where pt_date='${dt}'
;

---!!! (2024.03.04 Sample) 1 product SPL 24month, Nov 2023 & Dec 2023
---Part 2 starts here
--5. Create marginal ODR(observed default rate) 
--=IF(AND(H$141=1,$F142=5),1,IF(AND(H$141<>1,$F142=5),0,IF(SUM($G142:G142,MAX(0,H125/$G113))>1,0,MAX(0,H125/$G113))))

create table rr_test.test_rep_fin_ecl_pd_cohort_mrgl_odr_ss_m
(period         bigint
,m0_date string  --- 2024.03.07
,prod_desc      string
,tenor          string
,pd_cohort_ecl_bucket     string --2024.02.26
,odr_count_loan   decimal(36,10)
,odr_count_client decimal(36,10)
,odr_cur_balance   decimal(36,10)
,import_source      string
,ec_number          string)
partitioned by (pt_date string);

insert overwrite table rr_test.test_rep_fin_ecl_pd_cohort_mrgl_odr_ss_m
partition (pt_date='${dt}') 

select 
mt_default.period
,mt_default.m0_date  ---2024.03.07
,mt_default.prod_desc
,mt_default.tenor
,mt_default.pd_cohort_ecl_bucket    --2024.02.26
,case when mt_default.period=1 and mt_default.pd_cohort_ecl_bucket='5' then 1  
when mt_default.period<>1 and mt_default.pd_cohort_ecl_bucket='5' then 0
-- To handle total shall not exceed 100%: IF(SUM($G142:G142,MAX(0,H125/$G113))>1,0,MAX(0,H125/$G113)
when sum(nvl(greatest(nvl(mt_default.mrgl_count_loan/ mt_base.count_loan,0),0),0)) over accum_periods   > 1  
  then greatest(1- (sum(nvl(greatest(nvl(mt_default.mrgl_count_loan/ mt_base.count_loan,0),0),0)) over (accum_periods)   - nvl(greatest(nvl(mt_default.mrgl_count_loan/ mt_base.count_loan,0),0),0)),0)  
else nvl(greatest(nvl(mt_default.mrgl_count_loan/ mt_base.count_loan,0),0),0)  end as odr_count_loan  ---this is the final marginal odr %
,case when mt_default.period=1 and mt_default.pd_cohort_ecl_bucket='5' then 1
when mt_default.period<>1 and mt_default.pd_cohort_ecl_bucket='5' then 0
-- To handle total shall not exceed 100%: IF(SUM($G142:G142,MAX(0,H125/$G113))>1,0,MAX(0,H125/$G113)
when sum(nvl(greatest(nvl(mt_default.mrgl_count_client/ mt_base.count_client,0),0),0)) over accum_periods  > 1  
  then greatest(1- (sum(nvl(greatest(nvl(mt_default.mrgl_count_client/ mt_base.count_client,0),0),0)) over (accum_periods)  - nvl(greatest(nvl(mt_default.mrgl_count_client/ mt_base.count_client,0),0),0)),0)  
else nvl(greatest(nvl(mt_default.mrgl_count_client/ mt_base.count_client,0),0),0) end as odr_count_client
,case when mt_default.period=1 and mt_default.pd_cohort_ecl_bucket='5' then 1
when mt_default.period<>1 and mt_default.pd_cohort_ecl_bucket='5' then 0
-- To handle total shall not exceed 100%: IF(SUM($G142:G142,MAX(0,H125/$G113))>1,0,MAX(0,H125/$G113)
when sum(nvl(greatest(nvl(mt_default.mrgl_cur_balance/ mt_base.cur_balance,0),0),0)) over accum_periods   > 1 
   then greatest(1- (sum(nvl(greatest(nvl(mt_default.mrgl_cur_balance/ mt_base.cur_balance,0),0),0)) over (accum_periods)   - nvl(greatest(nvl(mt_default.mrgl_cur_balance/ mt_base.cur_balance,0),0),0)),0)
else nvl(greatest(nvl(mt_default.mrgl_cur_balance/ mt_base.cur_balance,0),0),0)  end as odr_cur_balance
,mt_default.import_source
,mt_default.ec_number
from
(select 
period
,m0_date   --- 2024.03.07
,prod_desc
,tenor
,pd_cohort_ecl_bucket ---2024.02.26
,mrgl_count_loan
,mrgl_count_client
,mrgl_cur_balance
,import_source
,ec_number
 from rr_test.test_rep_fin_ecl_pd_cohort_default_mrgl_ss_m
 where pt_date='${dt}') mt_default
left join 
(select 
prod_desc
,tenor
,pd_cohort_ecl_bucket ---2024.02.26
,count_loan 
,count_client 
,cur_balance  
,import_source 
,ec_number
,pt_date   
 from ecl.rep_fin_ecl_pd_cohort_population_agg_ss_m where pt_date<='${dt}') mt_base
on mt_default.m0_date=mt_base.pt_date
and mt_default.prod_desc=mt_base.prod_desc --- 2024.02.22PM remove prod_code upon confirmation with user and move to prod_desc
and mt_default.tenor=mt_base.tenor
and mt_default.pd_cohort_ecl_bucket=mt_base.pd_cohort_ecl_bucket
and mt_default.import_source=mt_base.import_source
and mt_default.ec_number=mt_base.ec_number
window accum_periods as (partition by mt_default.m0_date, mt_default.prod_desc, mt_default.tenor, mt_default.pd_cohort_ecl_bucket order by mt_default.period)
;  

--7. Create MPD from marginal ODR
create table rr_test.test_rep_fin_ecl_pd_cohort_mpd_ss_m
(period         bigint
,prod_desc    string
,tenor          string
,pd_cohort_ecl_bucket     string 
,pd_cohort_mpd_loan   decimal(36,10)
,pd_cohort_mpd_client decimal(36,10)
,pd_cohort_mpd_cur_balance   decimal(36,10))
partitioned by (pt_date string);

--amend 2024.04.16
with mpd as 
(select period 
    ,prod_desc
    ,tenor
    ,pd_cohort_ecl_bucket 
    ,avg(odr_count_loan)   as avg_pd_cohort_mpd_loan
    ,avg(odr_count_client) as avg_pd_cohort_mpd_client
    ,avg(odr_cur_balance)  as avg_pd_cohort_mpd_cur_balance
    ,import_source
    ,ec_number
    from rr_test.test_rep_fin_ecl_pd_cohort_mrgl_odr_ss_m 
    where pt_date='${dt}' 
      group by period     
      ,prod_desc
      ,tenor
      ,pd_cohort_ecl_bucket 
      ,import_source
      ,ec_number)
--(select mt_default.period
--              ,mt_default.prod_desc
--              ,mt_default.tenor
--              ,mt_default.pd_cohort_ecl_bucket
--              ,case when mt_default.period=1 and mt_default.pd_cohort_ecl_bucket='5' then 1  
--              when mt_default.period<>1 and mt_default.pd_cohort_ecl_bucket='5' then 0
--              -- To handle total shall not exceed 100%: IF(SUM($G142:G142,MAX(0,H125/$G113))>1,0,MAX(0,H125/$G113)
--              when sum(sum(greatest(mt_default.mrgl_count_loan,0)) /sum(mt_base.count_loan)) over accum_periods   > 1  --
--                then greatest(1- (sum(sum(greatest(mt_default.mrgl_count_loan,0)) /sum(mt_base.count_loan)) over (accum_periods) -  sum(greatest(mt_default.mrgl_count_loan,0)) /sum(mt_base.count_loan) ),0)
--              else sum(greatest(mt_default.mrgl_count_loan,0)) /sum(mt_base.count_loan)  end as pd_cohort_mpd_loan  ---this is the final marginal odr %
--              ,case when mt_default.period=1 and mt_default.pd_cohort_ecl_bucket='5' then 1
--              when sum(sum(greatest(mt_default.mrgl_count_client,0)) /sum(mt_base.count_client)) over accum_periods   > 1  --
--                then greatest(1- (sum(sum(greatest(mt_default.mrgl_count_client,0)) /sum(mt_base.count_client)) over (accum_periods) -  sum(greatest(mt_default.mrgl_count_client,0)) /sum(mt_base.count_client) ),0)
--              else sum(greatest(mt_default.mrgl_count_client,0)) /sum(mt_base.count_client)  end as pd_cohort_mpd_client  ---this is the final marginal odr %
--             ,case when mt_default.period=1 and mt_default.pd_cohort_ecl_bucket='5' then 1
--              when sum(sum(greatest(mt_default.mrgl_cur_balance,0)) /sum(mt_base.cur_balance)) over accum_periods   > 1  
--                then greatest(1- (sum(sum(greatest(mt_default.mrgl_cur_balance,0)) /sum(mt_base.cur_balance)) over (accum_periods) -  sum(greatest(mt_default.mrgl_cur_balance,0)) /sum(mt_base.cur_balance) ),0)
--              else sum(greatest(mt_default.mrgl_cur_balance,0)) /sum(mt_base.cur_balance)  end as pd_cohort_mpd_cur_balance ---this is the final marginal odr %
--             ,mt_default.import_source
--              ,mt_default.ec_number
--        from (
--        select 
--                period
--                ,m0_date
--                ,prod_desc
--                ,tenor
--                ,pd_cohort_ecl_bucket
--                ,mrgl_count_loan
--                ,mrgl_count_client
--                ,mrgl_cur_balance
--                ,import_source
--                ,ec_number
--                ,pt_date
--                from rr_test.test_rep_fin_ecl_pd_cohort_default_mrgl_ss_m
--                where pt_date='${dt}'      --2024.03.14
--                )  mt_default
--                    left join 
--                    (select 
--                        prod_desc
--                        ,tenor 
--                        ,pd_cohort_ecl_bucket  
--                        ,count_loan
--                        ,count_client
--                        ,cur_balance
--                        ,import_source
--                        ,ec_number
--                        ,pt_date   
--                        from rr_test.test_rep_fin_ecl_pd_cohort_population_agg_ss_m 
--                          where pt_date<='${dt}'    --2024.03.14
--                          ) mt_base
--                      on mt_default.m0_date=mt_base.pt_date
--                      and mt_default.prod_desc=mt_base.prod_desc
--                      and mt_default.tenor=mt_base.tenor
--                      and mt_default.pd_cohort_ecl_bucket=mt_base.pd_cohort_ecl_bucket  
--                      and mt_default.import_source=mt_base.import_source
--                      and mt_default.ec_number=mt_base.ec_number
--                      where add_months(mt_default.m0_date,5-cast(mt_default.pd_cohort_ecl_bucket as int)) <'${dt}'
--                        group by mt_default.period, mt_default.prod_desc,mt_default.tenor,mt_default.pd_cohort_ecl_bucket,mt_default.import_source,mt_default.ec_number,mt_default.pt_date
----window accum_periods as (partition by mt_default.prod_desc, mt_default.tenor, mt_default.pd_cohort_ecl_bucket order by mt_default.period)
--)

insert overwrite table rr_test.test_rep_fin_ecl_pd_cohort_mpd_ss_m
partition (pt_date='${dt}') 

select 
b.period 
,b.prod_desc 
,b.tenor
,a.ecl_bucket as pd_cohort_ecl_bucket
,nvl(c.pd_cohort_mpd_loan,0) as pd_cohort_mpd_loan
,nvl(c.pd_cohort_mpd_client,0) as pd_cohort_mpd_client
,nvl(c.pd_cohort_mpd_cur_balance,0) as pd_cohort_mpd_cur_balance
from 
-----data filler to ensure it's a 5 (ecl buckets) x n (periods) matrix
(select explode(ARRAY(1, 2, 3, 4, 5)) as ecl_bucket) a
left join
(select distinct period, prod_desc,tenor 
from mpd) b
on 1=1
left join 
(select period,prod_desc,tenor,pd_cohort_ecl_bucket,pd_cohort_mpd_loan,pd_cohort_mpd_client,pd_cohort_mpd_cur_balance,import_source,ec_number
from mpd) c
on a.ecl_bucket=c.pd_cohort_ecl_bucket
and b.period=c.period 
and b.prod_desc=c.prod_desc
and b.tenor=c.tenor
; 


--8. Create CPD from MPD 
--- accumulated for all historically PD to get cumulative result. 
create table rr_test.test_rep_fin_ecl_pd_cohort_cpd_ss_m
(period         bigint
,prod_desc  string
,tenor          string
,pd_cohort_ecl_bucket     string 
,pd_cohort_cpd_loan   decimal(36,10)
,pd_cohort_cpd_client decimal(36,10)
,pd_cohort_cpd_cur_balance   decimal(36,10) )
partitioned by (pt_date string);


insert overwrite table rr_test.test_rep_fin_ecl_pd_cohort_cpd_ss_m
partition (pt_date='${dt}') 
----As confirmed by user, cpd cal for pd_cohort is very diff from existing transition matrix 

select 
period
,prod_desc
,tenor    
,pd_cohort_ecl_bucket    
,case when sum(pd_cohort_mpd_loan) over accum_periods >1 then 1  --- accumulated all historical mpd and total need to be less than 1
else sum(pd_cohort_mpd_loan) over accum_periods end  as pd_cohort_cpd_loan 
,case when sum(pd_cohort_mpd_client) over accum_periods >1 then 1  --- accumulated all historical mpd and total need to be less than 1
else sum(pd_cohort_mpd_client) over accum_periods end as pd_cohort_cpd_client --- accumulated all historical mpd and total need to be less than 1 
,case when sum(pd_cohort_mpd_cur_balance) over accum_periods >1 then 1 
else sum(pd_cohort_mpd_cur_balance) over accum_periods end as pd_cohort_cpd_cur_balance   --- accumulated all historical mpd and total need to be less than 1   
from rr_test.test_rep_fin_ecl_pd_cohort_mpd_ss_m 
where pt_date='${dt}'
window accum_periods as (partition by prod_desc,tenor,pd_cohort_ecl_bucket order by period)
;

--8. Integration with PD param output 
CREATE TABLE rr_test.test_rep_fin_ecl_pd_cohort_param_output_ss_m(
       scenario string
    ,  pd_segment string
    ,  tenor  string --原本的data type INT
    ,  period  INT   
    ,  ecl_bucket_client  string --原本的data type INT
    ,  pd_base double
    ,  pd_best double
    ,  pd_worst double
    ,  cpd_base double
    ,  cpd_best double
    ,  cpd_worst double)
    PARTITIONED BY (pt_date  STRING);

insert overwrite table rr_test.test_rep_fin_ecl_pd_cohort_param_output_ss_m partition (pt_date = '${dt}')
select
        'BASE' as scenario
         , concat("Digital_",mpd.prod_desc) as pd_segment  -- need to change once BV incorporate into ECL cal
         , mpd.tenor
         , mpd.period
         , mpd.pd_cohort_ecl_bucket as ecl_bucket_client
         , case when pd_config.value='ACCT' then mpd.mpd_loan
          when pd_config.value= 'CLIENT' then mpd.mpd_client
          when pd_config.value = 'CUR_BALANCE' then mpd.mpd_cur_balance end 
          as  pd_base
         , 0 as pd_best
         , 0 as pd_worst
         , case when pd_config.value='ACCT' then cpd.cpd_loan
          when pd_config.value = 'CLIENT' then cpd.cpd_client
          when pd_config.value = 'CUR_BALANCE' then cpd.cpd_cur_balance end 
          as  cpd_base
          ,0 as cpd_best
          ,0 as cpd_worst
        from 
        (select 
        period
        ,prod_desc
        ,tenor
        ,pd_cohort_ecl_bucket 
        ,pd_cohort_mpd_loan as mpd_loan
        ,pd_cohort_mpd_client as mpd_client
        ,pd_cohort_mpd_cur_balance as mpd_cur_balance
        from  rr_test.test_rep_fin_ecl_pd_cohort_mpd_ss_m
    where pt_date = '${dt}' ) mpd
    left join 
    (select 
      period
        ,prod_desc
        ,tenor
        ,pd_cohort_ecl_bucket
        ,pd_cohort_cpd_loan as cpd_loan
        ,pd_cohort_cpd_client as cpd_client
        ,pd_cohort_cpd_cur_balance as cpd_cur_balance
    from  rr_test.test_rep_fin_ecl_pd_cohort_cpd_ss_m
    where pt_date = '${dt}' ) cpd
    on mpd.period=cpd.period 
      and mpd.prod_desc=cpd.prod_desc
      and mpd.tenor=cpd.tenor
      and mpd.pd_cohort_ecl_bucket =cpd.pd_cohort_ecl_bucket 
    left join      
    (SELECT
            function_key,VALUE
        FROM rr_test.test_dim_map_fin_ecl_master_config
        where function_key in ('PD_COHORT_CONFIG') and  '${dt}' between start_date and end_date
    )pd_config 
    on 1=1;

--temp test ecl_master_config
--create table rr_test.test_dim_map_fin_ecl_master_config (
--function_key	string
--,value	string
--,description	string	
--,value_type	string
--,remarks	string
--,last_modified_date	string
--,start_date	string
--,end_date	string)
--;

--insert into rr_test.test_dim_map_fin_ecl_master_config
--select 
--'PD_COHORT_CONFIG'  as function_key
--,'CLIENT' as value
--,'choose pd cohort at client level' as description
--,'' as value_type
--,'' as remarks
--,'2024-03-18' as last_modified_date
--,'1900-01-01' as start_date
--,'2999-12-31' as end_date;--





--- PD cohort Part 3 
--8.2 integration with term_structure and summary_output to generate pd_cohort live data
CREATE TABLE rr_test.test_rep_fin_ecl_ecl_pd_cohort_term_structure_ss_m(xxxx); 

--1. same table structure as ecl.rep_fin_ecl_ecl_term_structure_ss_m 
--2. sourcing from rr_test.test_rep_fin_ecl_pd_cohort_param_output_ss_m  instead of rep_fin_ecl_pd_param_output_ss_m_merge

----Q: 如何按照product type 来选择pd计算方法
----A: add new pd_method into map table 来选择pd计算方法

CREATE TABLE  rr_test.test_rep_fin_ecl_ecl_pd_cohort_summary_output_ss_m(xxxx);

--1. same table structure as ecl.rep_fin_ecl_ecl_summary_output_ss_m
--2. sourcing from rr_test.rep_fin_ecl_ecl_pd_cohort_term_structure_ss_m  instead of ecl.rep_fin_ecl_ecl_term_structure_ss_m 


select 
xxx
     , nvl((nvl(case when mfrseg.pd_method='PD_COHORT' then PD_COHORT_PD_OUTPUT.PD_BASE when mfrseg.pd_method='PD_PIT_TTC' then PD_OUTPUT.PD_BASE else 0 end,0) * MASTER_CONFIG.RATIO_BASE
    +nvl(PD_OUTPUT.PD_BEST,0) * MASTER_CONFIG.RATIO_BEST
    +nvl(PD_OUTPUT.PD_WORST,0) * MASTER_CONFIG.RATIO_WORST),0) PD_WEIGHTED
from
xxxx
 LEFT JOIN
     (
         Select
             PD_SEGMENT
              , PERIOD
              , ecl_bucket_client
              , tenor
              , sum(case when scenario = 'BASE'  then nvl(PD_BASE ,0) else 0 end) as  PD_BASE
         from rr_test.test_rep_fin_ecl_pd_cohort_param_output_ss_m
         where PT_DATE = last_day(add_months('{dt}',-1))
         group by
             PD_SEGMENT
             , PERIOD
             , ECL_BUCKET_CLIENT
             , TENOR
     ) PD_COHORT_PD_OUTPUT
     on MASTER_DATA.PD_SEGMENT = PD_COHORT_PD_OUTPUT.PD_SEGMENT
         and ECL_REPAYMENT_SCHEDULE.PERIOD = PD_COHORT_PD_OUTPUT.PERIOD  --- ok. above already amended to be in time sequence
         and MASTER_DATA.ECL_BUCKET_CLIENT= PD_COHORT_PD_OUTPUT.ecl_bucket_client
         and MASTER_DATA.tenor= PD_COHORT_PD_OUTPUT.tenor

         left join 
         (select distinct pd_segment,pd_method from dim_map_fin_reg_segment
         where '{{dt}}' between start_date and end_date) mfrseg
         on PD_COHORT_PD_OUTPUT.pd_segment = mfrseg.pd_segment


