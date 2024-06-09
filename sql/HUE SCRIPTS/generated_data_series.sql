-- Create Day Series up to 2012-01-31
create table id_finpro.calendar_day_series as with day_series as (
    select date_add("${start_day}", a.pos) as d
    from (
            select posexplode(
                    split(
                        repeat("o", datediff("${end_day}", "${start_day}")),
                        "o"
                    )
                )
        ) a
)
select d as d,
    year(d) as year,
    month(d) as month,
    day(d) as day,
    date_format(d, 'u') as daynumber_of_week,
    date_format(d, 'EEEE') as dayname_of_week,
    date_format(d, 'D') as daynumber_of_year
from day_series sort by d;
-- Create Month Series up to 2090-12-31
create table id_finpro.calendar_month_series as
select *
from id_finpro.calendar_day_series
where last_day(d) = d
order by d;