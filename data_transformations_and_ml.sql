  -- basic EDA of payouts data
SELECT
  COUNT(*) AS total_count,
  COUNT(DISTINCT platform_id) AS platform_count,
  COUNT(DISTINCT recipient_id) AS recipient_count,
  SUM(count) AS total_sum,
  SUM(amount) AS total_amount,
  MIN(amount) AS min_amount,
  MAX(amount) AS max_amount,
  COUNT(DISTINCT count) AS distinct_count,
  MIN(date) AS min_date,
  MAX(date) AS max_date
FROM
  `modern-sublime-215302.stripe.payouts`
  -- query to create base table with some added columns
CREATE OR REPLACE TABLE
  stripe.payouts_enriched AS
SELECT
  p.*,
  i.industry AS recipient_industry,
  c.country AS recipient_country,
  i2.industry AS platform_industry,
  c2.country AS platform_country,
  CASE
    WHEN p.amount < 0 THEN 1
    ELSE 0
END
  AS is_refund,
  EXTRACT(DAYOFWEEK
  FROM
    date) AS day_of_week,
  CASE
    WHEN SUM(CASE
      WHEN DATE(date) < '2018-03-01' THEN 1
      ELSE 0
  END
    ) OVER(PARTITION BY platform_id) = 0 THEN 1
    ELSE 0
END
  AS new_platform,
FROM
  stripe.payouts p
LEFT JOIN
  stripe.industries i
ON
  i.merchant_id = p.recipient_id
LEFT JOIN
  stripe.countries c
ON
  c.merchant_id = p.recipient_id
LEFT JOIN
  stripe.industries i2
ON
  i2.merchant_id = p.platform_id
LEFT JOIN
  stripe.countries c2
ON
  c2.merchant_id = p.platform_id
  -- creating time series by recipient_country to use for predictions, including date array to generate zeroes
  -- where there were no transactions for a recipient country on a given day
CREATE OR REPLACE TABLE
  stripe.payout_time_series AS
WITH
  dates AS (
  SELECT
    *
  FROM
    UNNEST(GENERATE_DATE_ARRAY(DATE('2018-01-01'),DATE('2018-12-31'))) AS date
  ORDER BY
    1 ),
  countries AS (
  SELECT
    DISTINCT recipient_country
  FROM
    stripe.payouts_enriched),
  dates_countries AS (
  SELECT
    recipient_country,
    date
  FROM
    countries
  CROSS JOIN
    dates
  ORDER BY
    1,
    2 ),
  sums AS (
  SELECT
    DATE(date) AS date,
    recipient_country,
    SUM(amount) AS daily_amount
  FROM
    stripe.payouts_enriched
  GROUP BY
    1,
    2 )
SELECT
  d.date,
  d.recipient_country,
  COALESCE(daily_amount,0) AS daily_amount
FROM
  dates_countries d
LEFT JOIN
  sums s
USING
  (date,
    recipient_country)
  -- model create statement using arima_plus
CREATE OR REPLACE MODEL
  stripe.forecast_by_country OPTIONS (model_type = 'ARIMA_PLUS',
    time_series_timestamp_col = 'date',
    time_series_data_col = 'daily_amount',
    time_series_id_col = 'recipient_country',
    horizon = 7,
    forecast_limit_lower_bound = 0,
    data_frequency = 'DAILY',
    holiday_region = 'US' ) AS
SELECT
  *
FROM
  stripe.payout_time_series
  -- use model to forecast daily amounts and then union time series to create continuous view
CREATE OR REPLACE TABLE
  stripe.forecasted_daily_amounts AS
SELECT
  DATE(forecast_timestamp) AS date,
  recipient_country,
  forecast_value/100 AS daily_amount,
  confidence_interval_lower_bound/100 AS ci_80_lower,
  confidence_interval_upper_bound/100 AS ci_80_upper,
FROM
  ML.FORECAST(MODEL stripe.forecast_by_country,
    STRUCT(7 AS horizon,
      0.8 AS confidence_level))
UNION ALL
SELECT
  date,
  recipient_country,
  daily_amount/100,
  NULL AS ci_80_lower,
  NULL AS ci_80_upper
FROM
  stripe.payout_time_series
ORDER BY
  2,
  1
  -- data transformtions used for product metrics
  -- create date array so that 'zeroes' exist for each day since each platform's first date in dataset
WITH
  dates AS (
  SELECT
    *
  FROM
    UNNEST(GENERATE_DATE_ARRAY(DATE('2018-01-01'),DATE('2018-12-31'))) AS date
  ORDER BY
    1 ),
  platforms AS (
  SELECT
    platform_id,
    MIN(date) AS min_date
  FROM
    stripe.payouts_enriched
  GROUP BY
    1),
  dates_platforms AS (
  SELECT
    platform_id,
    DATE(date) AS date
  FROM
    platforms
  CROSS JOIN
    dates
  WHERE
    DATE(date) >= DATE(min_date)
    -- remove platform with largest volume for comparison
    --  AND platform_id != 'id_5dded1fc8ff3f8c0d96019076394d2a7'
  ORDER BY
    1,
    2 ),
  sums AS (
  SELECT
    platform_id,
    DATE(date) AS date,
    platform_industry,
    platform_country,
    SUM(amount) AS total_amount,
    SUM(count) AS transactions,
    COUNT(DISTINCT recipient_id) AS recipients
  FROM
    stripe.payouts_enriched
    -- WHERE
    --   platform_id != 'id_5dded1fc8ff3f8c0d96019076394d2a7'
  GROUP BY
    1,
    2,
    3,
    4 ),
  combined AS (
  SELECT
    d.date,
    d.platform_id,
    s.platform_industry,
    s.platform_country,
    COALESCE(total_amount,0) AS total_amount,
    COALESCE(s.transactions,0) AS transactions,
    COALESCE(s.recipients,0) AS recipients
  FROM
    dates_platforms d
  LEFT JOIN
    sums s
  USING
    (date,
      platform_id) ),
  -- calculates distinct recipients and amounts for each platform over the prior 28 days (rolling)
  mar AS (
  SELECT
    d.date,
    d.platform_id,
    SUM(COUNT(DISTINCT recipient_id)) OVER(PARTITION BY d.platform_id ORDER BY d.date ASC ROWS BETWEEN 28 PRECEDING AND CURRENT ROW) AS distinct_recipients_last_28d,
    SUM(COUNT(DISTINCT recipient_id)) OVER(PARTITION BY d.platform_id ORDER BY d.date ASC) AS distinct_recipients_to_date,
    SUM(SUM(p.amount)) OVER(PARTITION BY d.platform_id ORDER BY d.date ASC ROWS BETWEEN 28 PRECEDING AND CURRENT ROW) AS total_amount_last_28d
  FROM
    dates_platforms d
  LEFT JOIN
    stripe.payouts_enriched p
  ON
    d.date = DATE(p.date)
    AND d.platform_id = p.platform_id
  GROUP BY
    1,
    2 ),
  aggs AS (
  SELECT
    c.*,
    CASE
      WHEN MIN(date) OVER(PARTITION BY platform_id) >= '2018-02-01' THEN MIN(date) OVER(PARTITION BY platform_id)
      ELSE NULL
  END
    AS first_transaction_date,
    CASE
      WHEN MIN(date) OVER(PARTITION BY platform_id) >= '2018-02-01' AND MIN(date) OVER(PARTITION BY platform_id) = date THEN 1
      ELSE 0
  END
    AS is_first_transaction_date,
    CASE
      WHEN MIN(date) OVER(PARTITION BY platform_id) >= '2018-02-01' THEN 1
      ELSE 0
  END
    AS is_new_platform_2018,
    SUM(total_amount) OVER(PARTITION BY platform_id ORDER BY date) AS total_amount_cumulative,
    SUM(transactions) OVER(PARTITION BY platform_id ORDER BY date) AS transactions_cumulative,
    MAX(total_amount) OVER(PARTITION BY platform_id ORDER BY date) AS max_daily_amount_to_date,
    mar.distinct_recipients_last_28d,
    mar.total_amount_last_28d,
    mar.distinct_recipients_to_date
  FROM
    combined c
  LEFT JOIN
    mar
  USING
    (date,
      platform_id) ),
  -- applies logic for various product adoption categories
  act_and_est AS (
  SELECT
    aggs.*,
    CASE
      WHEN total_amount_cumulative >= 10000 AND distinct_recipients_to_date >= 5 THEN 1
      ELSE 0
  END
    AS is_activated,
    CASE
      WHEN total_amount_last_28d >= 100000 AND distinct_recipients_to_date >= 5 THEN 1
      ELSE 0
  END
    AS is_established
  FROM
    aggs),
  daily_metrics AS (
  SELECT
    *,
    CASE
      WHEN is_activated = 0 THEN 1
      ELSE 0
  END
    AS is_unactivated,
    CASE
      WHEN total_amount_last_28d < 10000 AND SUM(is_activated) OVER(PARTITION BY platform_id ORDER BY date) > 0 THEN 1
      ELSE 0
  END
    AS is_abandoned,
    CASE
      WHEN total_amount_last_28d < 100000 AND SUM(is_established) OVER(PARTITION BY platform_id ORDER BY date) > 0 THEN 1
      ELSE 0
  END
    AS is_unestablished,
    CASE
      WHEN date = MIN(date) OVER(PARTITION BY platform_id ORDER BY is_activated DESC, date) THEN 1
      ELSE 0
  END
    AS is_activation_date,
    DATE_DIFF(MIN(date) OVER(PARTITION BY platform_id ORDER BY is_activated DESC, date),first_transaction_date,DAY) AS days_to_activation
  FROM
    act_and_est ),
  activation_time AS (
  SELECT
    LAST_DAY(first_transaction_date,MONTH) AS start_month,
    SUM(CASE
        WHEN days_to_activation > 7 THEN 1
        ELSE 0
    END
      ) AS new_platforms_activation_7d_plus,
    AVG(days_to_activation) AS avg_days_to_activation
  FROM
    daily_metrics
  WHERE
    is_activation_date = 1
    AND is_new_platform_2018 = 1
  GROUP BY
    1),
  -- for some metrics, takes calculations as of end of month
  eom_calcs AS (
  SELECT
    date,
    COUNT(DISTINCT platform_id) AS num_platforms,
    SUM(distinct_recipients_last_28d) AS distinct_recipients_last_28d,
    SUM(distinct_recipients_last_28d)/COUNT(DISTINCT platform_id) AS distinct_recipients_last_28d_per_platform,
    SUM(CASE
        WHEN date = LAST_DAY(first_transaction_date,MONTH) THEN 1
        ELSE 0
    END
      ) AS new_platforms
  FROM
    daily_metrics
  WHERE
    date = LAST_DAY(date, MONTH)
  GROUP BY
    1
  ORDER BY
    1 ),
  total_payments AS (
  SELECT
    LAST_DAY(date, MONTH) AS date,
    SUM(total_amount)/100 AS monthly_amount_usd
  FROM
    daily_metrics
  GROUP BY
    1 )
  -- select
  -- LAST_DAY(first_transaction_date,MONTH),
  -- avg(days_to_activation)
  -- from daily_metrics
  -- where is_activation_date = 1 and is_new_platform_2018 = 1 and days_to_activation > 0 and first_transaction_date < '2018-11-01'
  -- group by 1
  -- order by 1
SELECT
  ec.*,
  act.new_platforms_activation_7d_plus,
  act.avg_days_to_activation,
  tp.monthly_amount_usd,
  SAFE_DIVIDE(ec.num_platforms - LAG(ec.num_platforms,1) OVER(ORDER BY ec.date),LAG(ec.num_platforms,1) OVER(ORDER BY ec.date)) AS mom_platform_growth,
  SAFE_DIVIDE(tp.monthly_amount_usd - LAG(tp.monthly_amount_usd,1) OVER(ORDER BY ec.date),LAG(tp.monthly_amount_usd,1) OVER(ORDER BY ec.date)) AS mom_payments_growth,
  SAFE_DIVIDE(ec.num_platforms - LAG(ec.num_platforms,1) OVER(ORDER BY ec.date),LAG(ec.num_platforms,1) OVER(ORDER BY ec.date)) AS mom_platforms_growth,
  SAFE_DIVIDE(new_platforms_activation_7d_plus,ec.new_platforms) AS percent_new_platforms_7d_plus_act
FROM
  eom_calcs ec
LEFT JOIN
  activation_time act
ON
  act.start_month = ec.date
LEFT JOIN
  total_payments tp
ON
  tp.date = ec.date
ORDER BY
  1
  -- this section is used for product adoption counts and uses most of the above query, starting with daily metrics CTE
  -- product adoption metrics
  --   adoption AS (
  --   SELECT
  --     *,
  --     CASE
  --       WHEN is_established = 1 THEN 'Established'
  --       WHEN is_abandoned = 1 THEN 'Abandoned'
  --       WHEN is_unestablished = 1 THEN 'Un-Established'
  --       WHEN is_activated = 1 THEN 'Activated'
  --       WHEN is_unactivated = 1 THEN 'Unactivated'
  --   END
  --     AS product_adoption_category
  --   FROM
  --     daily_metrics )
  -- SELECT
  --   date,
  --   product_adoption_category,
  --   COUNT(DISTINCT platform_id) AS num_platforms
  -- FROM
  --   adoption
  -- WHERE
  --   date = LAST_DAY(date, MONTH)
  -- GROUP BY
  --   1,
  --   2
  -- create time series for education industry
CREATE OR REPLACE TABLE
  stripe.education_time_series AS
WITH
  dates AS (
  SELECT
    *
  FROM
    UNNEST(GENERATE_DATE_ARRAY(DATE('2018-01-01'),DATE('2018-12-31'))) AS date
  ORDER BY
    1 ),
  platforms AS (
  SELECT
    DISTINCT platform_id
  FROM
    stripe.payouts_enriched
  WHERE
    platform_industry = 'Education'
    AND new_platform = 0),
  dates_platforms AS (
  SELECT
    platform_id,
    date
  FROM
    platforms
  CROSS JOIN
    dates
  ORDER BY
    1,
    2 ),
  sums AS (
  SELECT
    DATE(date) AS date,
    platform_id,
    SUM(amount) AS daily_amount
  FROM
    stripe.payouts_enriched
  GROUP BY
    1,
    2 ),
  platform_index AS (
  SELECT
    platform_id,
    ROW_NUMBER() OVER() AS platform_index
  FROM
    platforms )
SELECT
  d.date,
  COALESCE(daily_amount,0) AS daily_amount,
  p.platform_index,
  ROW_NUMBER() OVER(PARTITION BY platform_id ORDER BY date) AS date_index
FROM
  dates_platforms d
LEFT JOIN
  sums s
USING
  (date,
    platform_id)
LEFT JOIN
  platform_index p
USING
  (platform_id)
  -- create time series by industry
CREATE OR REPLACE TABLE
  stripe.payout_time_series_industry AS
WITH
  dates AS (
  SELECT
    *
  FROM
    UNNEST(GENERATE_DATE_ARRAY(DATE('2018-01-01'),DATE('2018-12-31'))) AS date
  ORDER BY
    1 ),
  countries AS (
  SELECT
    DISTINCT platform_industry
  FROM
    stripe.payouts_enriched),
  dates_countries AS (
  SELECT
    platform_industry,
    date
  FROM
    countries
  CROSS JOIN
    dates
  ORDER BY
    1,
    2 ),
  sums AS (
  SELECT
    DATE(date) AS date,
    platform_industry,
    SUM(amount) AS daily_amount
  FROM
    stripe.payouts_enriched
  GROUP BY
    1,
    2 )
SELECT
  d.date,
  d.platform_industry,
  COALESCE(daily_amount,0) AS daily_amount
FROM
  dates_countries d
LEFT JOIN
  sums s
USING
  (date,
    platform_industry)
  -- create forecast model by industry with 365 day time horizon
CREATE OR REPLACE MODEL
  stripe.forecast_by_industry OPTIONS (model_type = 'ARIMA_PLUS',
    time_series_timestamp_col = 'date',
    time_series_data_col = 'daily_amount',
    time_series_id_col = 'platform_industry',
    horizon = 365,
    forecast_limit_lower_bound = 0,
    data_frequency = 'DAILY',
    holiday_region = 'US' ) AS
SELECT
  *
FROM
  stripe.payout_time_series_industry
  -- forecast daily amounts by industry for 2019
CREATE OR REPLACE TABLE
  stripe.forecasted_daily_amounts_industry AS
SELECT
  ROW_NUMBER() OVER(PARTITION BY platform_industry ORDER BY forecast_timestamp) AS day_of_year,
  platform_industry,
  forecast_value AS daily_amount
FROM
  ML.FORECAST(MODEL stripe.forecast_by_industry,
    STRUCT(365 AS horizon,
      0.9 AS confidence_level))
ORDER BY
  2,
  1
  -- create food & beverage time series
CREATE OR REPLACE TABLE
  stripe.fb_time_series AS
WITH
  dates AS (
  SELECT
    *
  FROM
    UNNEST(GENERATE_DATE_ARRAY(DATE('2018-01-01'),DATE('2018-12-31'))) AS date
  ORDER BY
    1 ),
  platforms AS (
  SELECT
    DISTINCT platform_id
  FROM
    stripe.payouts_enriched
  WHERE
    platform_industry = 'Food & Beverage'
    AND new_platform = 0),
  dates_platforms AS (
  SELECT
    platform_id,
    date
  FROM
    platforms
  CROSS JOIN
    dates
  ORDER BY
    1,
    2 ),
  sums AS (
  SELECT
    DATE(date) AS date,
    platform_id,
    SUM(amount) AS daily_amount
  FROM
    stripe.payouts_enriched
  GROUP BY
    1,
    2 ),
  platform_index AS (
  SELECT
    platform_id,
    ROW_NUMBER() OVER() AS platform_index
  FROM
    platforms )
SELECT
  d.date,
  COALESCE(daily_amount,0) AS daily_amount,
  p.platform_index,
  ROW_NUMBER() OVER(PARTITION BY platform_id ORDER BY date) AS date_index
FROM
  dates_platforms d
LEFT JOIN
  sums s
USING
  (date,
    platform_id)
LEFT JOIN
  platform_index p
USING
  (platform_id)
  -- create time series for hotels, restaurants, & leisure
CREATE OR REPLACE TABLE
  stripe.education_time_series AS
WITH
  dates AS (
  SELECT
    *
  FROM
    UNNEST(GENERATE_DATE_ARRAY(DATE('2018-01-01'),DATE('2018-12-31'))) AS date
  ORDER BY
    1 ),
  platforms AS (
  SELECT
    DISTINCT platform_id
  FROM
    stripe.payouts_enriched
  WHERE
    platform_industry IN ('Food & Beverage',
      'Travel & Hospitality')
    AND new_platform = 0),
  dates_platforms AS (
  SELECT
    platform_id,
    date
  FROM
    platforms
  CROSS JOIN
    dates
  ORDER BY
    1,
    2 ),
  sums AS (
  SELECT
    DATE(date) AS date,
    platform_id,
    SUM(amount) AS daily_amount
  FROM
    stripe.payouts_enriched
  GROUP BY
    1,
    2 ),
  platform_index AS (
  SELECT
    platform_id,
    ROW_NUMBER() OVER() AS platform_index
  FROM
    platforms )
SELECT
  d.date,
  COALESCE(daily_amount,0) AS daily_amount,
  p.platform_index,
  ROW_NUMBER() OVER(PARTITION BY platform_id ORDER BY date) AS date_index
FROM
  dates_platforms d
LEFT JOIN
  sums s
USING
  (date,
    platform_id)
LEFT JOIN
  platform_index p
USING
  (platform_id)
