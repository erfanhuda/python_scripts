-- COHORT CHECKING POPULATION FOR EC_NUMBER 2 (WRITE_OFF) & EC_NUMBER 1 (ACTIVE & DEFAULT)
select pt_date,
    count(loan_no)
from id_finpro.brd_m0_population
group by pt_date;
insert into id_finpro.brd_m0_population
select pt_date,
    loan_no,
    client_no,
    product_code,
    product_type,
    tenor,
    ec_number,
    ecl_bucket_acct,
    ecl_bucket_client,
    ecl_bucket_client_original,
    write_off_date,
    first_default_date,
    default_flag_acct,
    default_flag_client,
    case
        when ec_number = "EC1"
        and default_flag_acct = "N" then "Y"
        else "N"
    end as active_flag_acct,
    case
        when ec_number = "EC1"
        and default_flag_client = "N" then "Y"
        else "N"
    end as active_flag_client,
    case
        when ec_number = "EC2"
        and default_flag_acct = "Y" then "Y"
        else "N"
    end as wo_flag_acct,
    case
        when ec_number = "EC2"
        and default_flag_client = "Y" then "Y"
        else "N"
    end as wo_flag_client,
    int_real_rate,
    cur_balance
from ecl.rep_fin_ecl_prep_master_data_ss_m
where pt_date = "${date}"
    and ec_number = "EC2"
    and substring(write_off_date, 1, 7) = substring("${date}", 1, 7)
UNION ALL
select pt_date,
    loan_no,
    client_no,
    product_code,
    product_type,
    tenor,
    ec_number,
    ecl_bucket_acct,
    ecl_bucket_client,
    ecl_bucket_client_original,
    write_off_date,
    first_default_date,
    default_flag_acct,
    default_flag_client,
    case
        when ec_number = "EC1"
        and default_flag_acct = "N" then "Y"
        else "N"
    end as active_flag_acct,
    case
        when ec_number = "EC1"
        and default_flag_client = "N" then "Y"
        else "N"
    end as active_flag_client,
    case
        when ec_number = "EC2"
        and default_flag_acct = "Y" then "Y"
        else "N"
    end as wo_flag_acct,
    case
        when ec_number = "EC2"
        and default_flag_client = "Y" then "Y"
        else "N"
    end as wo_flag_client,
    int_real_rate,
    cur_balance
from ecl.rep_fin_ecl_prep_master_data_ss_m
where pt_date = "${date}"
    and ec_number = "EC1";
-- COHORT CHECKING POPULATION ACCORDINGLY FROM M0 POPULATION
with base_population as (
    select *
    from id_finpro.brd_m0_population
),
tracker_population as (
    select a.pt_date as date_population,
        b.pt_date,
        months_between(b.pt_date, a.pt_date) as period,
        b.loan_no,
        b.client_no,
        b.product_code,
        b.product_type,
        b.tenor,
        b.ec_number,
        b.ecl_bucket_acct,
        b.ecl_bucket_client,
        b.ecl_bucket_client_original,
        b.write_off_date,
        b.first_default_date,
        b.int_real_rate,
        b.cur_balance,
        b.default_flag_acct,
        b.default_flag_client,
        case
            when b.ec_number = "EC1"
            and b.default_flag_acct = "N" then "Y"
            else "N"
        end as active_flag_acct,
        case
            when b.ec_number = "EC1"
            and b.default_flag_client = "N" then "Y"
            else "N"
        end as active_flag_client,
        case
            when b.ec_number = "EC2"
            and b.default_flag_acct = "Y" then "Y"
            else "N"
        end as wo_flag_acct,
        case
            when b.ec_number = "EC2"
            and b.default_flag_client = "Y" then "Y"
            else "N"
        end as wo_flag_client
    from base_population a
        left join ecl.rep_fin_ecl_prep_master_data_ss_m b on a.loan_no = b.loan_no
        and b.pt_date > a.pt_date
        and b.pt_date = last_day(b.pt_date)
    where a.pt_date = "${date}"
),
cohort_aggregate_tracker as (
    select date_population,
        period,
        product_code,
        product_type,
        tenor,
        ec_number,
        ecl_bucket_acct,
        ecl_bucket_client,
        ecl_bucket_client_original,
        write_off_date,
        first_default_date,
        int_real_rate,
        default_flag_acct,
        default_flag_client,
        active_flag_acct,
        active_flag_client,
        wo_flag_acct,
        wo_flag_client,
        count(distinct loan_no) as count_loan,
        count(distinct client_no) as count_client,
        sum(cur_balance) as sum_balance
    from tracker_population
    where population_date = "${date}"
    group by date_population,
        period,
        product_code,
        product_type,
        tenor,
        ec_number,
        ecl_bucket_acct,
        ecl_bucket_client,
        ecl_bucket_client_original,
        write_off_date,
        first_default_date,
        int_real_rate,
        default_flag_acct,
        default_flag_client,
        active_flag_acct,
        active_flag_client,
        wo_flag_acct,
        wo_flag_client
),
check_count_rows as (
    select "base_population",
        count(*)
    from base_population
    where pt_date = "${date}"
    union all
    select "track_population_period_1",
        count(*)
    from tracker_population
    where period = 1
    union all
    select "track_population_period_2",
        count(*)
    from tracker_population
    where period = 2
    union all
    select "track_population_period_3",
        count(*)
    from tracker_population
    where period = 3
),
check_unmatch_account as (
    select a.loan_no as loan_base,
        a.ecl_bucket_client as bucket_base,
        b.loan_no as loan_track,
        b.ecl_bucket_client
    from base_population a
        cross join tracker_population b on a.loan_no = b.loan_no
        and b.pt_date = add_months(a.pt_date, cast(b.period as int))
    where b.ecl_bucket_client is null
)
select *
from tracker_population;
-- COHORT BASE AGGREGATE M0 POPULATION
select *,
    case
        when ec_number = "EC1"
        and default_flag_acct = "N" then "Y"
        else "N"
    end as active_flag_acct,
    case
        when ec_number = "EC1"
        and default_flag_client = "N" then "Y"
        else "N"
    end as active_flag_client,
    case
        when ec_number = "EC2"
        and default_flag_acct = "Y" then "Y"
        else "N"
    end as wo_flag_acct,
    case
        when ec_number = "EC2"
        and default_flag_client = "Y" then "Y"
        else "N"
    end as wo_flag_client
from (
        select pt_date,
            product_code,
            product_type,
            tenor,
            ec_number,
            ecl_bucket_acct,
            ecl_bucket_client,
            ecl_bucket_client_original,
            write_off_date,
            first_default_date,
            default_flag_acct,
            default_flag_client,
            int_real_rate,
            count(distinct loan_no) as count_loan,
            count(distinct client_no) as count_client,
            sum(cur_balance) as sum_balance
        from ecl.rep_fin_ecl_prep_master_data_ss_m
        where pt_date = "${date}"
            and ec_number = "EC1"
        group by pt_date,
            product_code,
            product_type,
            tenor,
            ec_number,
            write_off_date,
            first_default_date,
            ecl_bucket_acct,
            ecl_bucket_client,
            ecl_bucket_client_original,
            default_flag_acct,
            default_flag_client,
            int_real_rate
        UNION ALL
        select pt_date,
            product_code,
            product_type,
            tenor,
            ec_number,
            ecl_bucket_acct,
            ecl_bucket_client,
            ecl_bucket_client_original,
            write_off_date,
            first_default_date,
            default_flag_acct,
            default_flag_client,
            int_real_rate,
            count(distinct loan_no) as count_loan,
            count(distinct client_no) as count_client,
            sum(cur_balance) as sum_balance
        from ecl.rep_fin_ecl_prep_master_data_ss_m
        where pt_date = "${date}"
            and ec_number = "EC2"
            and substring(write_off_date, 1, 7) = substring("${date}", 1, 7)
        group by pt_date,
            product_code,
            product_type,
            tenor,
            ec_number,
            write_off_date,
            first_default_date,
            ecl_bucket_acct,
            ecl_bucket_client,
            ecl_bucket_client_original,
            default_flag_acct,
            default_flag_client,
            int_real_rate
    ) a;
-- COHORT BASE AGGREGATE MT POPULATION
select date_population,
    period,
    product_code,
    product_type,
    tenor,
    ec_number,
    ecl_bucket_acct,
    ecl_bucket_client,
    ecl_bucket_client_original,
    write_off_date,
    first_default_date,
    int_real_rate,
    default_flag_acct,
    default_flag_client,
    active_flag_acct,
    active_flag_client,
    wo_flag_acct,
    wo_flag_client,
    count(distinct loan_no) as count_loan,
    count(distinct client_no) as count_client,
    sum(cur_balance) as sum_balance
from tracker_population
where population_date = "${date}"
group by date_population,
    period,
    product_code,
    product_type,
    tenor,
    ec_number,
    ecl_bucket_acct,
    ecl_bucket_client,
    ecl_bucket_client_original,
    write_off_date,
    first_default_date,
    int_real_rate,
    default_flag_acct,
    default_flag_client,
    active_flag_acct,
    active_flag_client,
    wo_flag_acct,
    wo_flag_client;