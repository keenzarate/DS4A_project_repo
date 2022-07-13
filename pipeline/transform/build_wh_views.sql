/* cereal production in tonnes (maize, wheat, barley) (y1)
   and gas in metric tons (carbon dioxide, methane, nitrous oxide) (y2)
   by year (x) for each country (BRA, CHN, IND, USA) */

CREATE VIEW prod_wh.cereal_prod_gas_unit AS
SELECT DISTINCT
    data_year AS year,
    country_name AS country,
    cereal_name AS cereal,
    cereal_measure_value as cereal_production_tonnes,
    indicator_name AS gas,
    climate_gas_indicator_value as gas_metric_tons
FROM prod_wh.fact_cereal_and_climate
LEFT JOIN prod_wh.dim_country USING(country_id)
LEFT JOIN prod_wh.dim_cereals USING(cereal_id)
LEFT JOIN prod_wh.dim_element USING(element_id)
LEFT JOIN prod_wh.dim_climate_gas USING(indicator_id)
LEFT JOIN prod_wh.dim_time USING(time_id)
WHERE (cereal = 'Maize' OR cereal = 'Wheat' OR cereal = 'Barley')
    AND element_name = 'Production'
    AND (gas = 'Methane' OR gas = 'Carbon Dioxide' OR gas = 'Nitrous Oxide')
ORDER BY 5, 3, 2, 1


/* cereal production in tonnes (maize, wheat, barley) (y)
   by year (x) for each country (BRA, CHN, IND, USA) */

CREATE VIEW prod_wh.cereal_prod AS
SELECT DISTINCT
    data_year AS year,
    country_name AS country,
    cereal_name AS cereal,
    cereal_measure_value as cereal_production_tonnes
FROM prod_wh.fact_cereal_and_climate
LEFT JOIN prod_wh.dim_country USING(country_id)
LEFT JOIN prod_wh.dim_cereals USING(cereal_id)
LEFT JOIN prod_wh.dim_element USING(element_id)
LEFT JOIN prod_wh.dim_time USING(time_id)
WHERE (cereal = 'Maize' OR cereal = 'Wheat' OR cereal = 'Barley')
    AND element_name = 'Production'
ORDER BY 2, 3, 1


/* gas in metric tons (carbon dioxide, methane, nitrous oxide) (y)
   by year (x) for each country (BRA, CHN, IND, USA) */

CREATE VIEW prod_wh.gas_unit AS
SELECT DISTINCT
    data_year AS year,
    country_name AS country,
    indicator_name AS gas,
    climate_gas_indicator_value as gas_metric_tons
FROM prod_wh.fact_cereal_and_climate
LEFT JOIN prod_wh.dim_country USING(country_id)
LEFT JOIN prod_wh.dim_climate_gas USING(indicator_id)
LEFT JOIN prod_wh.dim_time USING(time_id)
WHERE (gas = 'Methane' OR gas = 'Carbon Dioxide' OR gas = 'Nitrous Oxide')
ORDER BY 2, 3, 1


/* sum of cereal production in tonnes (maize, wheat, barley) (y1)
   and sum of gas in metric tons (carbon dioxide, methane, nitrous oxide) (y2)
   by year (x) for each country (BRA, CHN, IND, USA) */

CREATE VIEW prod_wh.sum_cereal_gas AS
SELECT DISTINCT
    data_year AS year,
    SUM(cereal_measure_value) as sum_cereal_production_tonnes,
    SUM(climate_gas_indicator_value) as sum_gas_metric_tons
FROM prod_wh.fact_cereal_and_climate
LEFT JOIN prod_wh.dim_country USING(country_id)
LEFT JOIN prod_wh.dim_cereals USING(cereal_id)
LEFT JOIN prod_wh.dim_element USING(element_id)
LEFT JOIN prod_wh.dim_climate_gas USING(indicator_id)
LEFT JOIN prod_wh.dim_time USING(time_id)
WHERE (cereal_name = 'Maize' OR cereal_name = 'Wheat' OR cereal_name = 'Barley')
    AND element_name = 'Production'
    AND (indicator_name = 'Methane' OR indicator_name = 'Carbon Dioxide' OR indicator_name = 'Nitrous Oxide')
GROUP BY year
ORDER BY 1


/* build correlations table */

create or replace view PROJECT_TEAM_25.PROD_WH.CORRELATIONS(
	"YEAR",
	COUNTRY,
	BARLEY,
	MAIZE,
	WHEAT,
	CARBON_DIOXIDE,
	METHANE,
	NITROUS_OXIDE
) as
    SELECT DISTINCT
        DATA_YEAR AS "year",
        COUNTRY_NAME AS country,
        MAX(IFF(CEREAL_NAME='Barley', cereal_measure_value, NULL)) AS barley,
        MAX(IFF(CEREAL_NAME='Maize', cereal_measure_value, NULL)) AS maize,
        MAX(IFF(CEREAL_NAME='Wheat', cereal_measure_value, NULL)) AS wheat,
        MAX(IFF(INDICATOR_NAME='Carbon Dioxide', climate_gas_indicator_value, NULL)) AS carbon_dioxide,
        MAX(IFF(INDICATOR_NAME='Methane', climate_gas_indicator_value, NULL)) AS methane,
        MAX(IFF(INDICATOR_NAME='Nitrous Oxide', climate_gas_indicator_value, NULL)) AS nitrous_oxide        
    FROM prod_wh.fact_cereal_and_climate
    LEFT JOIN prod_wh.dim_country USING(country_id)
    LEFT JOIN prod_wh.dim_cereals USING(cereal_id)
    LEFT JOIN prod_wh.dim_element USING(element_id)
    LEFT JOIN prod_wh.dim_climate_gas USING(indicator_id)
    LEFT JOIN prod_wh.dim_time USING(time_id)
    WHERE (CEREAL_NAME = 'Maize' OR CEREAL_NAME = 'Wheat' OR CEREAL_NAME = 'Barley')
        AND ELEMENT_NAME = 'Production'
        AND (INDICATOR_NAME = 'Methane' OR INDICATOR_NAME = 'Carbon Dioxide' OR INDICATOR_NAME = 'Nitrous Oxide')
    GROUP BY 1, 2
    ORDER BY 1, 2;


/* COEFFICIENTS FOR BARLEY */
create or replace view PROJECT_TEAM_25.PROD_WH.BARLEY_COEFFICIENTS(
	BARLEY_GASES
) as
((SELECT CORR(BARLEY, CARBON_DIOXIDE)
FROM PROJECT_TEAM_25.PROD_WH.CORRELATIONS)
UNION
(SELECT CORR(BARLEY, METHANE)
FROM PROJECT_TEAM_25.PROD_WH.CORRELATIONS)
UNION
(SELECT CORR(BARLEY, NITROUS_OXIDE)
FROM PROJECT_TEAM_25.PROD_WH.CORRELATIONS));

/* COEFFICIENTS FOR c02 */
create or replace view PROJECT_TEAM_25.PROD_WH.CO2_COEFFICIENTS(
	BARLEY,
	MAIZE,
	WHEAT
) as
(SELECT CORR(BARLEY, CARBON_DIOXIDE), CORR(MAIZE, CARBON_DIOXIDE), CORR(WHEAT, CARBON_DIOXIDE) FROM CORRELATIONS);


/* COEFFICIENTS FOR MAIZE */
create or replace view PROJECT_TEAM_25.PROD_WH.MAIZE_COEFFICIENTS(
	MAIZE_GASES
) as
((SELECT CORR(MAIZE, CARBON_DIOXIDE)
FROM PROJECT_TEAM_25.PROD_WH.CORRELATIONS)
UNION
(SELECT CORR(MAIZE, METHANE)
FROM PROJECT_TEAM_25.PROD_WH.CORRELATIONS)
UNION
(SELECT CORR(MAIZE, NITROUS_OXIDE)
FROM PROJECT_TEAM_25.PROD_WH.CORRELATIONS));

/* COEFFICIENTS FOR N20 */
create or replace view PROJECT_TEAM_25.PROD_WH.N2O_COEFFICIENTS(
	BARLEY,
	MAIZE,
	WHEAT
) as
    (SELECT CORR(BARLEY, NITROUS_OXIDE), CORR(MAIZE, NITROUS_OXIDE), CORR(WHEAT, NITROUS_OXIDE) FROM CORRELATIONS);

/* COEFFICIENTS FOR WHEAT */
create or replace view PROJECT_TEAM_25.PROD_WH.WHEAT_COEFFICIENTS(
	WHEAT_GASES
) as
((SELECT CORR(WHEAT, CARBON_DIOXIDE)
FROM PROJECT_TEAM_25.PROD_WH.CORRELATIONS)
UNION
(SELECT CORR(WHEAT, METHANE)
FROM PROJECT_TEAM_25.PROD_WH.CORRELATIONS)
UNION
(SELECT CORR(WHEAT, NITROUS_OXIDE)
FROM PROJECT_TEAM_25.PROD_WH.CORRELATIONS));


/* COMBINE ALL COEFFICIENTS IN ONE TABLE */
create or replace view PROJECT_TEAM_25.PROD_WH.COEFFICIENTS(
    GAS,
	BARLEY,
	MAIZE,
	WHEAT
) as
SELECT 'C02' as gas, BARLEY, MAIZE, WHEAT FROM CO2_COEFFICIENTS
UNION ALL
SELECT 'CH4' as gas,  BARLEY, MAIZE, WHEAT FROM CH4_COEFFICIENTS
UNION ALL
SELECT 'N20' as gas,  BARLEY, MAIZE, WHEAT FROM N2O_COEFFICIENTS;