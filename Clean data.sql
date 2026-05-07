CREATE OR REPLACE TABLE `cpsc482-final.weather_energy.fact_hourly_clean` AS
SELECT * REPLACE(
  CASE 
    WHEN demand_mwh < 0 OR demand_mwh > 500000 THEN NULL 
    ELSE demand_mwh 
  END AS demand_mwh
)
FROM `cpsc482-final.weather_energy.fact_hourly`