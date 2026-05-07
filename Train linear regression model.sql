CREATE OR REPLACE MODEL `cpsc482-final.weather_energy.model_linear_regression`
OPTIONS (
  model_type = "LINEAR_REG",
  input_label_cols = ["demand_mwh"]
) AS
SELECT
  demand_mwh, temp_weighted, humidity_avg, wind_avg, solar_avg,
  hdh, cdh, precip_total, cloud_avg,
  hour_of_day, day_of_week, month, is_weekend, balancing_authority
FROM `cpsc482-final.weather_energy.fact_hourly`
WHERE split = "train" AND demand_mwh IS NOT NULL