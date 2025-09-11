WITH 
origin AS (
	SELECT origin origin_airport_code
		,dest dest_airport_code
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
totals AS (
SELECT a1.city, a1.country, a1.name, o.origin_airport_code, a2.name, a2.city, a2.country, o.dest_airport_code
	,unique_airplanes
	,unique_airlines
	,average_elapsed_time
	,average_arr_delay
	,max_arr_delay
	,min_arr_delay
	,total_cancelled
	,total_diverted
FROM origin o
LEFT JOIN  {{ref('prep_airports')}} a1 ON o.origin_airport_code = a1.faa 
LEFT JOIN  {{ref('prep_airports')}} a2 ON o.dest_airport_code = a2.faa
)
SELECT * FROM totals