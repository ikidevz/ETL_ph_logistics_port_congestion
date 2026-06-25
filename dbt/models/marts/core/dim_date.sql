{{
  config(materialized='table')
}}

WITH date_spine AS (
    SELECT
        DATEADD('day', SEQ4(), '2020-01-01'::DATE)  AS date_key
    FROM TABLE(GENERATOR(ROWCOUNT => 4018))
    WHERE date_key <= '2030-12-31'::DATE
)
SELECT
    date_key,
    YEAR(date_key)                                  AS year_num,
    QUARTER(date_key)                               AS quarter_num,
    MONTH(date_key)                                 AS month_num,
    MONTHNAME(date_key)                             AS month_name,
    WEEK(date_key)                                  AS week_num,
    DAY(date_key)                                   AS day_of_month,
    DAYOFWEEK(date_key)                             AS day_of_week,
    DAYNAME(date_key)                               AS day_name,
    CASE WHEN DAYOFWEEK(date_key) IN (0, 6)
         THEN TRUE ELSE FALSE END                   AS is_weekend,
    TO_CHAR(date_key, 'YYYY-MM')                   AS year_month,
    TO_CHAR(date_key, 'YYYY-"Q"Q')                AS year_quarter
FROM date_spine