{{ config(materialized='table') }}
 
select
    tf.Datetime,
    sd.`Station ID`,
    tf.Entries,
    tf.Exits,
    sl.booth
 
 
 
from
    `data_management_2.NYC_subway_traffic_2017-2021` tf
    left join {{ ref('Stations_Dimension') }} sd
        on tf.`Stop Name` = sd.`Stop Name`
 
    -- booth
    left join data_management_2.station_name_mapping snm
    on lower(tf.`Stop Name`) = lower(snm.headsign_text)
    left join data_management_2.station_lookup as sl
    on (
        replace(lower(sl.station),"st ", "st. ") = replace(replace(lower(tf.`Stop Name`), "av", "ave"), "st ", "st. ")
        or lower(sl.station) = lower(snm.station))