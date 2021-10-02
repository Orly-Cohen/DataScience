
select * from Coronel_Pacheco_climate_KNN

ALTER TABLE Coronel_Pacheco_climate_KNN
DROP COLUMN column1;

select * from Juiz_de_fora_climate_KNN

select * from Manhuacu_climate_KNN

select * from Vicosa_climate_KNN

select * from Muriae_climate_KNN
 
select * from medical_data

--=========================
-- Arranging medical table
--=========================
-- add column for Dengue: yes(1) or no (0)
-- add opposite column 'other disease': yes(1) no (0)
-- fixing city names to fit climate tables
-- add column for city density (people/km2)
-- add column for city(station) altitude (meters)

-- DROP VIEW medical_1

CREATE VIEW  medical_1 AS
SELECT *,
(CASE WHEN (ID_AGRAVO = 'A90') THEN (1) ELSE (0) END) AS Dengue,
(CASE WHEN (ID_AGRAVO <> 'A90') THEN (1) ELSE (0) END) AS Other_Disease,
CASE
	WHEN ID_MUNICIP = 313940 THEN 'Manhuacu'
	WHEN ID_MUNICIP = 317130 THEN 'Vicosa'
	WHEN ID_MUNICIP = 313670 THEN 'Juiz_de_fora'
	WHEN ID_MUNICIP = 314390 THEN 'Muriae'
	WHEN ID_MUNICIP = 311960 THEN 'Coronel_Pacheco'
END AS city,
CASE
	WHEN ID_MUNICIP = 313940 THEN 145.1
	WHEN ID_MUNICIP = 317130 THEN 263.3
	WHEN ID_MUNICIP = 313670 THEN 396.2
	WHEN ID_MUNICIP = 314390 THEN 129.2
	WHEN ID_MUNICIP = 311960 THEN 23.5
END AS city_dens,
CASE
	WHEN ID_MUNICIP = 313940 THEN 819.47
	WHEN ID_MUNICIP = 317130 THEN 697.64
	WHEN ID_MUNICIP = 313670 THEN 936.88
	WHEN ID_MUNICIP = 314390 THEN 282.79
	WHEN ID_MUNICIP = 311960 THEN 411.14
END AS altitude
FROM medical_data

SELECT * FROM  medical_1

-- DROP TABLE medical_2
-- sum number of cases in week

SELECT  city, city_dens, altitude, weekofyear, 
SUM(Dengue) AS Dengue_cases,
SUM(Other_Disease) AS non_Dengue_cases
INTO medical_2
FROM medical_1
GROUP BY city,  city_dens, altitude, weekofyear
ORDER BY city, city_dens, altitude, weekofyear

SELECT * FROM  medical_2
ORDER BY city,  weekofyear

--=======================================
-- Joining all cilmate tables to one view
--=======================================
-- DROP VIEW climate_all

CREATE VIEW climate_all AS

select * from Juiz_de_fora_climate_KNN
union all
select * from Manhuacu_climate_KNN
union all
select * from Muriae_climate_KNN
union all
select * from Vicosa_climate_KNN
union all
select * from Coronel_Pacheco_climate_KNN;

select * from climate_all




--===============================================================
-- create view of all cilmate data by week of the year (average) 
-- ==============================================================

CREATE VIEW climate_perweek AS

SELECT city, weekofyear, 
AVG(max_daily_temp_c) as max_temp, 
AVG(min_daily_temp_c) as min_temp,
AVG((max_daily_temp_c) - (min_daily_temp_c)) AS DTR, -- diurnal temperature range
SUM(total_daily_rainfall_aut_mm) AS rain,
AVG(average_daily_dew_point_temp_c) AS dew_point,
AVG(daily_average_atmospheric_pressure_aut_m_b) AS atm_pressure,
AVG(average_daily_air_relative_humidity_aut) AS avg_humidity,
AVG(minimum_daily_air_humidity_aut) AS min_humidity,
AVG(wind_max_daily_gust_ms) AS Gust,
AVG(wind_average_daily_speed_ms) AS avg_wind,
MAX([year]) AS [year],
MIN([month]) AS [month],
MAX([week]) AS [week]
FROM climate_all
GROUP BY city, weekofyear;

GO

select * from climate_perweek
ORDER BY city, weekofyear;

--===================
-- left join tables for
-- final falt file
--===================
-- DROP TABLE Dengue_flat_file2

SELECT 
t1.city AS city_name, 
max_temp, 
min_temp,
DTR,
rain, 
dew_point, 
atm_pressure, 
avg_humidity, 
min_humidity, 
Gust, 
avg_wind,  
t1.weekofyear AS weekofyear, 
[year], 
[month],
[week],
t2.altitude,
t2.city_dens,
ISNULL(t2.non_Dengue_cases, 0) AS non_Dengue_cases, -- weeks with no reported cases will be considered as 0 cases
ISNULL(t2.Dengue_cases, 0) AS Dengue_cases -- weeks with no reported cases will be considered as 0 cases
INTO Dengue_flat_file2
FROM climate_perweek AS t1
LEFT JOIN medical_2 AS t2
ON t1.city = t2.city
AND t1.weekofyear = t2.weekofyear
ORDER BY t1.city, t1.weekofyear

SELECT * FROM Dengue_flat_file2
ORDER BY city_name, [year], [week]



