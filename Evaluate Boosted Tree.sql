SELECT *
FROM ML.EVALUATE(MODEL `cpsc482-final.weather_energy.model_boosted_tree`,
  (SELECT demand_mwh, temp_weighted, humidity_avg, wind_avg, solar_avg,
          hdh, cdh, precip_total, cloud_avg,
          hour_of_day, day_of_week, month, is_weekend, balancing_authority
   FROM `cpsc482-final.weather_energy.fact_hourly`
   WHERE split = 'eval' AND demand_mwh IS NOT NULL))