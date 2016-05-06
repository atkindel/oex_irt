SELECT 'time', 'anon_screen_name', 'event_type', 'course_start', 'start_delta', 'course_end', 'end_delta'
UNION ALL
SELECT min(et.`time`), et.anon_screen_name, et.event_type, ci.start_date as course_start, (unix_timestamp(et.`time`) - unix_timestamp(ci.start_date)) as start_delta, ci.end_date as course_end, (unix_timestamp(et.`time`) - unix_timestamp(ci.end_date)) as end_delta
INTO OUTFILE '/home/dataman/Data/CustomExcerpts/SU_Kindel_IRT_raws/{0}_Registrations.csv'
	FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
	LINES TERMINATED BY '\n'
FROM Edx.EdxTrackEvent et, Edx.CourseInfo ci
WHERE et.course_display_name = '{1}'
AND et.course_display_name = ci.course_display_name
AND event_type != 'about'  # First event that wasn't just looking at the registration page
GROUP BY anon_screen_name
ORDER BY `time`;
