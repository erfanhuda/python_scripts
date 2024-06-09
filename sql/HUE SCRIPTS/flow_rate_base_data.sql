-- Write Off
select *
from (
        select pt_date,
            product_type,
            tenor_in_month,
            int_real_rate,
            wo_principal,
            clawback_principal,
            sum(wo_principal) over (
                partition by substring(pt_date, 1, 7),
                product_type,
                tenor_in_month,
                int_real_rate
                order by pt_date,
                    product_type,
                    tenor_in_month,
                    int_real_rate,
                    wo_principal
            ) wo_mtd,
            sum(clawback_principal) over (
                partition by substring(pt_date, 1, 7),
                product_type,
                tenor_in_month,
                int_real_rate
                order by pt_date,
                    product_type,
                    tenor_in_month,
                    int_real_rate,
                    clawback_principal
            ) clawback_mtd
        from id_finpro.flow_rate_base_wo
        where pt_date between '${start_date}' and '${end_date}'
            and (
                wo_principal <> 0
                or clawback_principal <> 0
            )
    ) a
order by pt_date,
    product_type,
    tenor_in_month,
    int_real_rate,
    wo_mtd,
    clawback_mtd;
select *
from (
        select pt_date,
            product_type,
            tenor_in_month,
            wo_principal,
            clawback_principal,
            sum(wo_principal) over (
                partition by substring(pt_date, 1, 7),
                product_type,
                tenor_in_month
                order by pt_date,
                    product_type,
                    tenor_in_month,
                    wo_principal
            ) wo_mtd,
            sum(clawback_principal) over (
                partition by substring(pt_date, 1, 7),
                product_type,
                tenor_in_month
                order by pt_date,
                    product_type,
                    tenor_in_month,
                    clawback_principal
            ) clawback_mtd
        from id_finpro.flow_rate_base_wo
        where pt_date between '${start_date}' and '${end_date}'
            and (
                wo_principal <> 0
                or clawback_principal <> 0
            )
    ) a
order by pt_date,
    product_type,
    tenor_in_month,
    wo_mtd,
    clawback_mtd;
select *
from (
        select pt_date,
            product_type,
            wo_principal,
            clawback_principal,
            sum(wo_principal) over (
                partition by substring(pt_date, 1, 7),
                product_type
                order by pt_date,
                    product_type,
                    wo_principal
            ) wo_mtd,
            sum(clawback_principal) over (
                partition by substring(pt_date, 1, 7),
                product_type
                order by pt_date,
                    product_type,
                    clawback_principal
            ) clawback_mtd
        from id_finpro.flow_rate_base_wo
        where pt_date between '${start_date}' and '${end_date}'
            and (
                wo_principal <> 0
                or clawback_principal <> 0
            )
    ) a
order by pt_date,
    product_type,
    wo_mtd,
    clawback_mtd;
-- Disbursement
select *
from (
        select pt_date,
            product_type,
            principal,
            sum(principal) over (
                partition by substring(pt_date, 1, 7),
                product_type
                order by pt_date,
                    product_type,
                    principal
            ) prin_mtd
        from id_finpro.flow_rate_base_disburse
        where pt_date between '${start_date}' and '${end_date}'
            and principal <> 0
    ) a
order by pt_date,
    product_type,
    principal;
select *
from (
        select pt_date,
            product_type,
            tenor_in_month,
            principal,
            sum(principal) over (
                partition by substring(pt_date, 1, 7),
                product_type,
                tenor_in_month
                order by pt_date,
                    product_type,
                    tenor_in_month,
                    principal
            ) prin_mtd
        from id_finpro.flow_rate_base_disburse
        where pt_date between '${start_date}' and '${end_date}'
            and principal <> 0
    ) a
order by pt_date,
    product_type,
    principal;
select *
from (
        select pt_date,
            product_type,
            tenor_in_month,
            int_real_rate,
            principal,
            sum(principal) over (
                partition by substring(pt_date, 1, 7),
                product_type,
                tenor_in_month
                order by pt_date,
                    product_type,
                    tenor_in_month
            ) prin_mtd
        from id_finpro.flow_rate_base_disburse
        where pt_date between '${start_date}' and '${end_date}'
            and principal <> 0
    ) a
order by pt_date,
    product_type,
    principal;