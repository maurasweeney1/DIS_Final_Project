CREATE OR REPLACE TABLE `cpsc482-final.weather_energy.predictions_2023` AS
SELECT *
FROM ML.PREDICT(MODEL `cpsc482-final.weather_energy.model_boosted_tree`,
  (SELECT * FROM `cpsc482-final.weather_energy.fact_hourly`
   WHERE split = 'test' AND demand_mwh IS NOT NULL))