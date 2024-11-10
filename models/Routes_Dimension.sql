{{ config(materialized='table') }}
 
 
select
    sl.booth, m.route_id as line, m.direction, m.headsign_text, sl.division
FROM
    data_management_2.metro m
    left join data_management_2.station_name_mapping snm
    on lower(m.headsign_text) = lower(snm.headsign_text)
    left join data_management_2.station_lookup as sl
    on (
        replace(lower(sl.station),"st ", "st. ") = replace(replace(lower(m.headsign_text), "av", "ave"), "st ", "st. ")
        or lower(sl.station) = lower(snm.station))
where
    m.headsign_text != ""