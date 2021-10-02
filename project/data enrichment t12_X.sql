--=======================
-- Data enrichment
--=======================
-- create columns for biting rate (ordinal) according to max temperature and min temperature
-- creat column for biting rate category (sum of minimum and maximum)
-- use lag to create columns for climate data of previous weeks
-- use lag to create column of number of cases of previous weeks
-- DROP VIEW table_1


SELECT * FROM initial_flat_file_t12;

CREATE VIEW table_1_t12 AS
SELECT *,
CASE
	WHEN max_temp < 10 THEN 0
	WHEN max_temp BETWEEN 10 AND 14.999 THEN 1
	WHEN max_temp BETWEEN 15 AND 24.999 THEN 2
	WHEN max_temp BETWEEN 14.9999 AND 34.999 THEN 3
	WHEN max_temp BETWEEN 35 AND 39.999 THEN 1
	WHEN max_temp >40  THEN 0
END AS BR_maxT, -- biting rate category for daily maximum temperature
CASE
	WHEN min_temp < 10 THEN 0
	WHEN min_temp BETWEEN 10 AND 14.999 THEN 1
	WHEN min_temp BETWEEN 15 AND 24.999 THEN 2
	WHEN min_temp BETWEEN 25 AND 34.999 THEN 3
	WHEN min_temp BETWEEN 35 AND 39.999 THEN 1
	WHEN min_temp >40  THEN 0
END AS BR_minT -- biting rate category for daily minimum temperature
FROM initial_flat_file_t12;



CREATE VIEW final_ff_t12_X AS

SELECT 
city_name,
weekofyear,
[week],
altitude,
city_dens,
LAG(max_temp, 2) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [max_temp_t-2],
LAG(min_temp, 2) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [min_temp_t-2],
LAG(DTR, 2) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [DTR_t-2],
LAG(BR_maxT, 2) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [BR_maxT_t-2],
LAG(BR_minT, 2) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [BR_minT_t-2],
LAG(([BR_maxT] + [BR_minT]), 2) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [BR_t-2],
LAG(rain_log, 2) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [rain_log_t-2],
LAG(dew_point, 2) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [dew_point_t-2],
LAG(sea_level_pressure, 2) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [sea_level_pressure_t-2],
LAG(avg_humidity, 2) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [avg_humidity_t-2],
LAG(min_humidity, 2) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [min_humidity_t-2],
LAG(Gust, 2) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [Gust_t-2],
LAG(avg_wind, 2) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [avg_wind_t-2],
LAG(max_temp, 4) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [max_temp_t-4],
LAG(min_temp, 4) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [min_temp_t-4],
LAG(DTR, 4) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [DTR_t-4],
LAG(BR_maxT, 4) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [BR_maxT_t-4],
LAG(BR_minT, 4) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [BR_minT_t-4],
LAG(([BR_maxT] + [BR_minT]), 4) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [BR_t-4],
LAG(rain_log, 4) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [rain_log_t-4],
LAG(dew_point, 4) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [dew_point_t-4],
LAG(sea_level_pressure, 4) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [sea_level_pressure_t-4],
LAG(avg_humidity, 4) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [avg_humidity_t-4],
LAG(min_humidity, 4) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [min_humidity_t-4],
LAG(Gust, 4) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [Gust_t-4],
LAG(avg_wind, 4) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [avg_wind_t-4],
LAG(max_temp, 6) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [max_temp_t-6],
LAG(min_temp, 6) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [min_temp_t-6],
LAG(DTR, 6) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [DTR_t-6],
LAG(BR_maxT, 6) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [BR_maxT_t-6],
LAG(BR_minT, 6) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [BR_minT_t-6],
LAG(([BR_maxT] + [BR_minT]), 6) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [BR_t-6],
LAG(rain_log, 6) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [rain_log_t-6],
LAG(dew_point, 6) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [dew_point_t-6],
LAG(sea_level_pressure, 6) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [sea_level_pressure_t-6],
LAG(avg_humidity, 6) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [avg_humidity_t-6],
LAG(min_humidity, 6) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [min_humidity_t-6],
LAG(Gust, 6) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [Gust_t-6],
LAG(avg_wind, 6) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [avg_wind_t-6],
LAG(max_temp, 8) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [max_temp_t-8],
LAG(min_temp, 8) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [min_temp_t-8],
LAG(DTR, 8) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [DTR_t-8],
LAG(BR_maxT, 8) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [BR_maxT_t-8],
LAG(BR_minT, 8) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [BR_minT_t-8],
LAG(([BR_maxT] + [BR_minT]), 8) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [BR_t-8],
LAG(rain_log, 8) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [rain_log_t-8],
LAG(dew_point, 8) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [dew_point_t-8],
LAG(sea_level_pressure, 8) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [sea_level_pressure_t-8],
LAG(avg_humidity, 8) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [avg_humidity_t-8],
LAG(min_humidity, 8) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [min_humidity_t-8],
LAG(Gust, 8) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [Gust_t-8],
LAG(avg_wind, 8) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [avg_wind_t-8],
LAG(max_temp, 10) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [max_temp_t-10],
LAG(min_temp, 10) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [min_temp_t-10],
LAG(DTR, 10) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [DTR_t-10],
LAG(BR_maxT, 10) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [BR_maxT_t-10],
LAG(BR_minT, 10) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [BR_minT_t-10],
LAG(([BR_maxT] + [BR_minT]), 10) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [BR_t-10],
LAG(rain_log, 10) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [rain_log_t-10],
LAG(dew_point, 10) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [dew_point_t-10],
LAG(sea_level_pressure, 10) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [sea_level_pressure_t-10],
LAG(avg_humidity, 10) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [avg_humidity_t-10],
LAG(min_humidity, 10) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [min_humidity_t-10],
LAG(Gust, 10) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [Gust_t-10],
LAG(avg_wind, 10) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [avg_wind_t-10],
LAG(max_temp, 12) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [max_temp_t-12],
LAG(min_temp, 12) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [min_temp_t-12],
LAG(DTR, 12) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [DTR_t-12],
LAG(BR_maxT, 12) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [BR_maxT_t-12],
LAG(BR_minT, 12) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [BR_minT_t-12],
LAG(([BR_maxT] + [BR_minT]), 12) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [BR_t-12],
LAG(rain_log, 12) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [rain_log_t-12],
LAG(dew_point, 12) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [dew_point_t-12],
LAG(sea_level_pressure, 12) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [sea_level_pressure_t-12],
LAG(avg_humidity, 12) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [avg_humidity_t-12],
LAG(min_humidity, 12) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [min_humidity_t-12],
LAG(Gust, 12) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [Gust_t-12],
LAG(avg_wind, 12) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [avg_wind_t-12],
LAG(Dengue_cases, 12) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [Dengue_cases_t-12],
LAG(non_Dengue_cases, 12) OVER(PARTITION BY city_name ORDER BY weekofyear) AS [non_Dengue_cases_t-12],
Dengue_cases
FROM table_1_t12;

SELECT * FROM final_ff_t12_X
order BY city_name, weekofyear