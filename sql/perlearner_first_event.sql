SELECT 'time', 'anon_screen_name', 'event_type'
UNION ALL
SELECT min(`time`), anon_screen_name, event_type
INTO OUTFILE '/home/dataman/Data/CustomExcerpts/SU_Kindel_IRT_raws/{0}_Registrations.csv'
	FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
	LINES TERMINATED BY '\n'
FROM Edx.EdxTrackEvent
WHERE course_display_name = '{1}'
AND event_type = 'edx.course.enrollment.activated'
GROUP BY anon_screen_name;
