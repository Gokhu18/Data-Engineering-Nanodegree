insert_queries = {
'immigration_facts':("""
INSERT INTO immigration_facts(cicid,i94port,i94addr,dtadfile,dtaddto,admnum)
    SELECT 
        cast(cast(cicid as real) as bigint) cicid,
        i94port,
        i94addr,
        to_date(dtadfile, 'yyyymmdd') as dtadfile,
        case when dtaddto <> 'D/S' then to_date(dtaddto, 'mmddyyyy') else null end as dtaddto,
        cast(cast(admnum as real) as bigint) admnum
    FROM staging_immigration
"""),
'states':("""
INSERT INTO states(state_code, state_name)
    SELECT distinct
        state_code,
        state state_name
    FROM staging_demographics
"""),
'cities':("""
INSERT INTO cities(city_name, state_code, total_pop)
    SELECT distinct
        city city_name,
        state_code,
        cast(cast(total_population as real) as bigint) total_pop
    FROM staging_demographics
"""),
'times':("""
INSERT INTO times(date,day,month,year,weekday)
    SELECT distinct * from (
        SELECT
            dtadfile as date,
            extract(day from dtadfile) as day,
            extract(month from dtadfile) as month,
            extract(year from dtadfile) as year,
            extract(dayofweek from dtadfile) as weekday
        FROM immigration_facts
        UNION ALL
        SELECT
            dtaddto as date,
            extract(day from dtaddto) as day,
            extract(month from dtaddto) as month,
            extract(year from dtaddto) as year,
            extract(dayofweek from dtaddto) as weekday
        FROM immigration_facts
        ) t
        where date is not null
"""),
}