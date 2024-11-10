{{ config(materialized='table') }}
 
SELECT
  `Datetime` AS `Date Key`,
  EXTRACT(YEAR FROM `Datetime`) AS year,
  EXTRACT(MONTH FROM `Datetime`) AS month,
  EXTRACT(DAY FROM `Datetime`) AS day,
  EXTRACT(QUARTER FROM `Datetime`) AS quarter,
  CASE
    WHEN EXTRACT(DAYOFWEEK FROM `Datetime`) IN (1, 7) THEN 'Weekend'
    ELSE 'Weekday'
  END AS weekday_weekend,
   EXTRACT(HOUR FROM `Datetime`) AS time_of_day
FROM
  `data_management_2.NYC_subway_traffic_2017-2021` as nst
 