-- create schemas 
create schema clean_data;
create schema prod_wh;


-- create tables to store clean data CEREAL
create table clean_data.cereal_data(
	country_id varchar, 
	country varchar, 
	element_code varchar, 
	element varchar, 
	item_id varchar,
	item varchar,
	year varchar, 
	unit varchar, 
	value numeric
);

-- create tables to store clean data greenhouse gas
create table clean_data.greenhouse_gas (
	 country varchar,             
	 sector  varchar,
	 gas     varchar,
	 unit    varchar,
	 year    varchar,
	 measure_value numeric
);

-- create tables to store clean data climate indicator
create table clean_data.climate_indicator (
	year varchar,      
	country  varchar,  
	location varchar, 
	measure_value numeric,           
	measure_name varchar,
	unit varchar
);

-- create tables to store clean data world_cities
create table clean_data.world_cities(
	country varchar, 
	number_of_cities numeric,
	total_population numeric	
);


-- Update S3 URI and aws key and secret key
copy into clean_data.cereal_data
from S3 URI 
credentials=(aws_key_id='' aws_secret_key='')
FILE_FORMAT = (TYPE = 'csv') ON_ERROR = 'CONTINUE';


copy into clean_data.climate_indicator
from S3 URI 
credentials=(aws_key_id='' aws_secret_key='')
FILE_FORMAT = (TYPE = 'csv') ON_ERROR = 'CONTINUE';


copy into clean_data.world_cities
from S3 URI
credentials=(aws_key_id='' aws_secret_key='')
FILE_FORMAT = (TYPE = 'csv') ON_ERROR = 'CONTINUE';


copy into clean_data.greenhouse_gas
from S3 URI
credentials=(aws_key_id='' aws_secret_key='')
FILE_FORMAT = (TYPE = 'csv') ON_ERROR = 'CONTINUE';


-- BUILD ALL DIMENSION TABLES 
create table prod_wh.dim_element(
	element_id varchar primary key,
	element_name varchar,
	unit varchar,
	unit_descr varchar
);


create table prod_wh.dim_country (
  country_id varchar primary key,
  country_name varchar,
  number_of_cities numeric, 
  current_total_pop numeric
);


create table prod_wh.dim_cereals (
  cereal_id varchar primary key,
  cereal_name varchar
);

create table prod_wh.dim_time(
  time_id varchar primary key,
  data_year varchar
);


create table prod_wh.int_climate_indicator(
  indicator_id varchar primary key,
  indicator_name varchar,
  indicator_abbrev varchar,
  unit varchar,
  unit_descr varchar
);


create table prod_wh.int_greenhouse_gas(
  indicator_id varchar primary key,
  indicator_name varchar,
  indicator_abbrev varchar,
  unit varchar,
  unit_descr varchar
  
);


create table prod_wh.dim_climate_gas  (
	indicator_id varchar primary key,
    indicator_name varchar,
    indicator_abbrev varchar,
    unit varchar,
    unit_descr varchar);
   
   

create table prod_wh.int_fact_cereal_agg (
  country_id  varchar,
  time_id     varchar,
  element_id  varchar,
  cereal_id   varchar,
  cereal_measure_value integer,
  FOREIGN key (country_id) REFERENCES prod_wh.dim_country(country_id),
  FOREIGN key (time_id) REFERENCES prod_wh.dim_time(time_id),
  FOREIGN key (element_id) REFERENCES prod_wh.dim_element(element_id),
  FOREIGN key (cereal_id) REFERENCES prod_wh.dim_cereals(cereal_id)
  
);



create table prod_wh.int_fact_climate_agg (
  country_id  varchar,
  time_id     varchar,
  indicator_id varchar,
  climate_gas_indicator_value numeric,
  FOREIGN key (country_id) REFERENCES prod_wh.dim_country(country_id),
  FOREIGN key (time_id) REFERENCES prod_wh.dim_time(time_id),
  FOREIGN key (indicator_id) REFERENCES prod_wh.dim_climate_gas(indicator_id));


 
 
 /* insert values into the dimension and fact tables */

insert into prod_wh.dim_element (select 
 	distinct
 	md5(element), 
 	element, 
 	unit, 
 	case
 		when unit = 'hg/ha' then 'hectogram per hectar'
 		when unit = 'tonnes' then 'tonnes'
 		when unit = 'ha' then 'hectar'
 	end as unit_descr
   from clean_data.cereal_data);



insert into prod_wh.dim_country (
with dt_country as (
select 
 	distinct 
 	country
 from clean_data.climate_indicator
 union all 
 select 
	distinct 
    country
 from clean_data.greenhouse_gas
 union all 
 select 
	distinct 
    country
 from clean_data.cereal_data
)
select distinct 
      md5(dt.country), 
      dt.country, 
      number_of_cities, 
      total_population
from dt_country dt
left join clean_data.world_cities wc on dt.country = wc.country );


insert into prod_wh.dim_cereals (
select 
 	distinct
 	md5(item), 
 	item
   from clean_data.cereal_data);


insert into prod_wh.dim_time (
with dt_year as 
(select 
 	distinct 
 	year
 from clean_data.climate_indicator
 union all 
 select 
	distinct 
    year
 from clean_data.greenhouse_gas
 union all 
 select 
	distinct 
    year
 from clean_data.cereal_data
)
select distinct 
      md5(year), 
      year
from dt_year
order by year);


insert into prod_wh.int_climate_indicator (
select 
 	distinct
 	md5(measure_name), 
 	measure_name,
 	case 
 		when measure_name = 'Precipitation' then 'PRECIP'
		when measure_name = 'Mean Temperature' then 'TEMP'
	end as indicator_abbrev,
 	case 
 		when unit = 'Celcius' then 'C'
 		when unit = 'Tonnes' then 'tons'
 	end as unit,
 	unit
   from clean_data.climate_indicator);

  
insert into prod_wh.int_greenhouse_gas (
	select 
		distinct 
		md5(gas), 
		case 
	 		when gas = 'CH4' then 'Methane'
			when gas = 'CO2' then 'Carbon Dioxide'
			when gas = 'N2O' then 'Nitrous Oxide'
	    end as indicator_name,
		gas as gas_abbrev, 
		unit,
		case 
	 		when unit = 'MtCO₂e' then 'Metric tons of carbon dioxide equivalent'
 		end as unit
    from clean_data.greenhouse_gas gg 
);



insert into prod_wh.dim_climate_gas (
	select *
	from prod_wh.int_climate_indicator ici 
	union all 
	select *
	from prod_wh.int_greenhouse_gas igg 

);


insert into prod_wh.int_fact_cereal_agg (
	select 
		distinct 
		dc2.country_id, 
		dt.time_id, 
		de.element_id, 
		dc.cereal_id, 
		value
	from clean_data.cereal_data cd 
	left join prod_wh.dim_cereals dc on cd.item = dc.cereal_name
	left join prod_wh.dim_country dc2 on cd.country = dc2.country_name
	left join prod_wh.dim_time dt on cd.year = dt.data_year 
	left join prod_wh.dim_element de on cd.element = de.element_name );
	


insert into prod_wh.int_fact_climate_agg (
	with gas_greenhouse as (select 
		distinct 
		dc2.country_id, 
		dt.time_id, 
		de.indicator_id, 
		measure_value
	from clean_data.greenhouse_gas cd 
	left join prod_wh.dim_country dc2 on cd.country = dc2.country_name
	left join prod_wh.dim_time dt on cd.year = dt.data_year 
	left join prod_wh.dim_climate_gas de on cd.gas = de.indicator_abbrev 
    ),
    climate_ind as (
    select 
		distinct 
		dc2.country_id, 
		dt.time_id, 
		dcii.indicator_id, 
		measure_value
	from clean_data.climate_indicator cd 
	left join prod_wh.dim_country dc2 on cd.country = dc2.country_name
	left join prod_wh.dim_time dt on cd.year = dt.data_year 
	left join prod_wh.dim_climate_gas dcii on cd.measure_name = dcii.indicator_name
	where country = location)
	select * 
    from gas_greenhouse
    union all 
    select * 
    from climate_ind);

  
   --- create a view table 
   create table prod_wh.fact_cereal_and_climate (
    country_id  varchar,
     time_id     varchar,
     indicator_id varchar,
     element_id  varchar,
     cereal_id   varchar,
     cereal_measure_value integer,
     climate_gas_indicator_value numeric,
  FOREIGN key (country_id) REFERENCES prod_wh.dim_country(country_id),
  FOREIGN key (time_id) REFERENCES prod_wh.dim_time(time_id),
  FOREIGN key (indicator_id) REFERENCES prod_wh.dim_climate_gas(indicator_id),
  FOREIGN key (element_id) REFERENCES prod_wh.dim_element(element_id),
  FOREIGN key (cereal_id) REFERENCES prod_wh.dim_cereals(cereal_id));


  -- insert data in fact table 
  insert into prod_wh.fact_cereal_and_climate (select distinct 
          fca.country_id, 
   		  fca.time_id, 
   		  indicator_id, 
   		  element_id, 
   		  cereal_id,
   		  cereal_measure_value, 
   		  climate_gas_indicator_value
   from prod_wh.int_fact_cereal_agg fca 
   left join prod_wh.int_fact_climate_agg fca2 on fca.country_id = fca2.country_id 
   										  and fca.time_id   = fca2.time_id);
   										  
   					


--  drops -- run only if we need to delete and redo everything
-- drop table clean_data.cereal_data cascade; 
-- drop table clean_data.climate_indicator cascade; 
-- drop table clean_data.greenhouse_gas cascade; 
-- drop table clean_data.world_cities cascade;
-- drop table prod_wh.dim_element cascade;
-- drop table prod_wh.dim_country cascade;
-- drop table prod_wh.dim_cereals cascade;
-- drop table prod_wh.dim_time cascade;
-- drop table prod_wh.int_climate_indicator cascade; 
-- drop table prod_wh.int_greenhouse_gas  cascade;
-- drop table prod_wh.dim_climate_gas cascade;
-- drop table prod_wh.int_fact_cereal_agg;
-- drop table prod_wh.int_fact_climate_agg;
-- drop table prod_wh.fact_cereal_and_climate;


-- to test 
 select * 
 from prod_wh.fact_cereaL_and_climate fca
 left join prod_wh.dim_element de using(element_id)
 left join prod_wh.dim_climate_gas dcg using(indicator_id)
 left join prod_wh.dim_time using(time_id)
 where fca.time_id = '84ddfb34126fc3a48ee38d7044e87276' and element_id = '756d97bb256b8580d4d71ee0c547804e' 
and cereal_id = 'a4b4e5b1b17e841ac1842fc69762210a' and fca.country_id = '42537f0fb56e31e20ab9c2305752087d'
 
   