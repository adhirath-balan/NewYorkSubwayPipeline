{{ config(materialized='table') }}
 
 
select `Station ID`,  `Stop Name`, `Borough`, `GTFS Stop ID`, `GTFS Latitude`, `GTFS Longitude`, `North Direction Label`, `South Direction Label`
from data_management_2.Stations