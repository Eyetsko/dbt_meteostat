WITH 
flight_stats AS (
	SELECT origin
		,dest
		,count(*)
		,count(DISTINCT tail_number) unique_airplanes
		,count(DISTINCT airline) unique_airlines
		,round(avg(actual_elapsed_time)) average_elapsed_time
		,round(avg(arr_delay)) average_arr_delay
		,max(arr_delay) max_arr_delay
		,min(arr_delay) min_arr_delay
		,max(dep_delay) max_dep_delay
		,min(dep_delay) min_dep_delay
		,SUM(cancelled) total_cancelled
		,SUM(diverted) total_diverted
	FROM {{ref('prep_flights')}}
	GROUP BY origin, dest
),
add_names AS (
	SELECT o.city origin_city
		,d.city dest_city
		,o.name origin_name
		,d.name dest_name
		,f.*
	FROM flight_stats f
	LEFT JOIN {{ref('prep_airports')}} o
		ON o.faa = origin
	LEFT JOIN {{ref('prep_airports')}} d
		ON d.faa = dest
		)
SELECT *
FROM add_names
ORDER BY (origin, dest) DESC