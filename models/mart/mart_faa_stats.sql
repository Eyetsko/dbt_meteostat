WITH departures AS (
	SELECT dest airport_code
		,count(DISTINCT origin) unique_from
		,count(sched_arr_time) arr_planned
		,SUM(cancelled) arr_cancelled
		,SUM(diverted) arr_diverted
		,count(arr_time) actual_arr
	FROM {{ref('prep_flights')}}
	GROUP BY dest
),
arrivals AS (
	SELECT origin airport_code
		,count(DISTINCT dest) unique_to
		,count(sched_dep_time) dep_planned
		,SUM(cancelled) dep_cancelled
		,SUM(diverted) dep_diverted
		,count(dep_time) actual_dep
	FROM {{ref('prep_flights')}}
	GROUP BY origin
),
total_stats AS (
SELECT 
	airport_code
	,unique_to
	,unique_from
	,(arr_planned + dep_planned) total_planned
	,(arr_cancelled + dep_cancelled) total_cancelled
	,(arr_diverted + dep_diverted) total_diverted
	,(actual_arr + actual_dep) total_flights
FROM departures
JOIN arrivals
USING (airport_code)
)
SELECT pa.city, pa.country, pa.name, 
	ts.*
FROM total_stats ts
JOIN {{ref('prep_flights')}}
ON ts.airport_code = faa