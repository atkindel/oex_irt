SELECT 'anon_screen_name', 'event_type', 'time'
UNION ALL
SELECT anon_screen_name, event_type, `time`
INTO OUTFILE '/home/dataman/Data/CustomExcerpts/SU_Kindel_IRT_raws/{0}_ViewProgress.csv'
	FIELDS ESCAPED BY '"' TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
	LINES TERMINATED BY '\n'
FROM Edx.EdxTrackEvent
WHERE event_type LIKE '%progress%'
AND course_display_name = '{1}'
