select *
from ecl.rep_fin_ecl_tms_loss_rate_ss_m
where pt_date = '2024-03-31'
    and rating = 'AAA';
select *
from ecl.rep_fin_ecl_repayment_schedule_ss_m
where client_no = '100008864'
    and pt_date = '2024-04-30';
select *
from ecl.rep_fin_ecl_ecl_term_structure_ss_m
where pt_date in ('2024-05-06', '2024-05-07', '2024-04-30')
    and client_no = '100008864';
select *
from ecl.rep_fin_ecl_ecl_term_structure_ss_m
where pt_date = '2024-05-07'
    and pd_segment = 'Digital_SPL'
    and tenor = 12
    and client_no = '103869573';
select *
from ecl.rep_fin_ecl_ecl_term_structure_ss_m
where pt_date = '2024-05-07'
    and pd_segment = 'Digital_SPL'
    and tenor = 18
    and client_no = '103869573';
select *
from ecl.rep_fin_ecl_ecl_summary_output_ss_m
where pt_date in ('2024-05-06', '2024-05-07', '2024-04-30')
    and client_no = '100008864';
select *
from ecl.rep_fin_ecl_repayment_schedule_ss_m
where client_no = '100008864'
    and pt_date = '2024-04-30';
select *
from ecl.rep_fin_ecl_ecl_term_structure_ss_m
where pt_date in ('2024-05-06', '2024-05-07', '2024-04-30')
    and client_no = '100008864';
select *
from ecl.rep_fin_ecl_ecl_term_structure_ss_m
where pt_date = '2024-05-07'
    and pd_segment = 'Digital_SPL'
    and tenor = 12
    and client_no = '103869573';
select *
from ecl.rep_fin_ecl_ecl_term_structure_ss_m
where pt_date = '2024-05-07'
    and pd_segment = 'Digital_SPL'
    and tenor = 18
    and client_no = '100008864';
select *
from ecl.rep_fin_ecl_ecl_summary_output_ss_m
where pt_date in ('2024-05-06', '2024-05-07', '2024-04-30')
select *
from ecl.rep_fin_ecl_ecl_summary_output_ss_m
where pt_date = '2024-04-30'
    and ecl_stage_client = 2
    and tenor = 18;
select distinct pt_date
from ecl.rep_fin_ecl_ecl_term_structure_ss_m;
select *
from ecl.rep_fin_ecl_pd_param_output_ss_m_merge
where pt_date = '2024-03-31'
    and pd_segment = 'Digital_EML'
    and tenor = 3
    and ecl_bucket_client = 1
    and period <= 12
    and scenario = 'WORST';
select *
from ecl.rep_fin_ecl_lgd_param_output_ss_m_merge
where pt_date = '2024-03-31'
    and pd_segment = 'Digital_KPL';
select *
from ecl.rep_fin_ecl_ecl_summary_output_ss_m
where pt_date = '2024-05-07'
    and pd_segment = 'Digital_SPL'
    and tenor = 18;
describe dm.rep_fin_reg_com_master_kredit_ss_d;
select max(pt_date)
from ecl.rep_fin_ecl_ecl_summary_output_ss_m;
create table id_finpro.sementara_flow_rate_base_disburse as with date_series AS (
    select date_add(
            concat(date_format('${start_date}', 'yyyy-MM'), '-01'),
            a.pos
        ) as dateseries
    from (
            select posexplode(
                    split(
                        repeat(
                            "o",
                            datediff(
                                last_day('${end_date}'),
                                concat(date_format('${start_date}', 'yyyy-MM'), '-01')
                            )
                        ),
                        "o"
                    )
                )
        ) a -- from (select posexplode(split(repeat("o", datediff(last_day(date_add(current_date(), -1)),concat(date_format(date_add(current_date(), -1), 'yyyy-MM'), '-01'))), "o"))) a
),
disburse_check AS (
    select pt_date,
        loan_no,
        principal,
        is_reversal,
        prod_type,
        tenor,
        -- tenor_in_month,
        -- payment_freq,
        nvl(
            tenor_in_month,
            case
                when prod_type in ('108', '109', '110') then round(months_between(loan_maturity_date, pt_Date), 0)
                else tenor
            end
        ) as tenor_in_month,
        nvl(
            payment_freq,
            case
                when prod_type in ('108', '109', '110') then round(
                    round(months_between(loan_maturity_date, pt_date), 0) / tenor,
                    0
                )
                else 1
            end
        ) as payment_freq,
        int_real_rate,
        count(loan_no) over (partition by loan_no) as count_loan,
        count(loan_no) over (partition by loan_no, pt_date) as count_loan_in_pt_date
    from dm.rep_fin_reg_loan_disburse_flow_ss_d
    where pt_date between date_add(date_format('${start_date}', 'yyyy-MM-01'), -10) and '${end_date}' -- where pt_date between date_format(date_add(current_date(), -1),'yyyy-MM-01') and date_add(current_date(), -1)
        and disburse_status <> 'LOAN_FAIL'
    UNION ALL
    select loan_disbursement_date as pt_date,
        loan_no,
        loan_disb,
        "N" as is_reversal,
        product_code as prod_type,
        tenor,
        tenor as tenor_in_month,
        1 as payment_freq,
        int_real_rate,
        count(loan_no) over (partition by loan_no) as count_loan,
        count(loan_no) over (partition by loan_no, loan_disbursement_date) as count_loan_in_pt_date
    from dm.rep_fin_reg_com_master_kredit_ss_d
    where loan_disbursement_date = pt_date
        and pt_date between date_format('${start_date}', 'yyyy-MM-01') and '${end_date}' -- and pt_date between date_format(date_add(current_date(), -1),'yyyy-MM-01') and date_add(current_date(), -1)
        and product_code in ('SC1', 'SC2', 'M05', 'C01')
),
summary_disburse_daily as (
    select pt_date,
        prod_type as product_code,
        case
            when prod_type in ('101', '102') then 'SPL'
            when prod_type in ('103', '104') then 'BCL'
            when prod_type = '105' then 'SCL'
            when prod_type = '106' then 'KPL'
            when prod_type = '107' then 'EML'
            when prod_type = '108' then 'RCL'
            when prod_type = '109' then 'AKL'
            when prod_type = '110' then 'EAL'
            when prod_type = '111' then 'PYL'
            when prod_type = '112' then 'UDL'
            when prod_type = '115' then 'APL'
            when prod_type = 'C01' then 'APL'
            when prod_type = 'SC1' then 'SCF'
            when prod_type = 'SC2' then 'SCF'
            when prod_type = 'M05' then 'SME'
        END AS PRODUCT_TYPE,
        tenor,
        tenor_in_month,
        payment_freq,
        int_real_rate,
        sum(
            case
                when count_loan = 3
                and count_loan_in_pt_date = 2
                and is_reversal = 'N' then principal * 0
                when is_reversal = 'Y' then principal * -1
                else principal
            end
        ) as principal_new
    from disburse_check
    group by pt_date,
        prod_type,
        case
            when prod_type in ('101', '102') then 'SPL'
            when prod_type in ('103', '104') then 'BCL'
            when prod_type = '105' then 'SCL'
            when prod_type = '106' then 'KPL'
            when prod_type = '107' then 'EML'
            when prod_type = '108' then 'RCL'
            when prod_type = '109' then 'AKL'
            when prod_type = '110' then 'EAL'
            when prod_type = '111' then 'PYL'
            when prod_type = '112' then 'UDL'
            when prod_type = '115' then 'APL'
            when prod_type = 'C01' then 'APL'
            when prod_type = 'SC1' then 'SCF'
            when prod_type = 'SC2' then 'SCF'
            when prod_type = 'M05' then 'SME'
        END,
        tenor,
        tenor_in_month,
        payment_freq,
        int_real_rate
),
master_disburse as (
    select distinct a.product_code,
        a.product_type,
        a.tenor,
        a.tenor_in_month,
        a.payment_freq,
        a.int_real_rate,
        b.dateseries as pt_date
    from summary_disburse_daily a
        cross join date_series b
),
master_final as (
    select a.*,
        nvl(b.principal_new, 0) as principal_new
    from master_disburse a
        left join summary_disburse_daily b on a.pt_date = b.pt_date
        and a.product_code = b.product_code
        and a.product_type = b.product_type
        and a.tenor = b.tenor
        and a.tenor_in_month = b.tenor_in_month
        and a.payment_freq = b.payment_freq
        and a.int_real_rate = b.int_real_rate
    where a.pt_date between date_format('${start_date}', 'yyyy-MM-01') and '${end_date}' -- where a.pt_date between date_format(date_add(current_date(),-1),'yyyy-MM-01')  and date_add(current_date(),-1)
) -- insert overwrite table id_finpro.flow_rate_base_disburse partition (pt_date)
select product_code,
    product_type,
    tenor,
    tenor_in_month,
    payment_freq,
    int_real_rate,
    principal_new,
    sum(principal_new) over (
        partition by substring(pt_date, 1, 7),
        product_code,
        product_type,
        tenor,
        tenor_in_month,
        payment_freq,
        int_real_rate
        order by pt_date rows unbounded preceding
    ) as principal_mtd,
    pt_date
from master_final
where pt_date between '${start_date}' and '${end_date}';
-- where pt_date = date_add(current_date(),-1);
drop table id_finpro.sementara_flow_rate_base_active;
drop table id_finpro.sementara_flow_rate_base_wo;
select *
from id_finpro.flow_rate_base_active_no_buyback;
select *
from id_finpro.sementara_flow_rate_base_active_no_buyback;
select distinct buyback_flag
from id_finpro.sementara_flow_rate_base_active_no_buyback;
describe id_finpro.flow_rate_base_active_without_buyback;
describe id_finpro.flow_rate_base_active_no_buyback;
describe id_finpro.sementara_flow_rate_base_active_no_buyback;
select max(pt_date)
from id_finpro.flow_rate_view_client_summary_pricing;