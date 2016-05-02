SELECT ex.event_id, ex.event_type, ex.anon_screen_name, ex.time, ex.problem_id
INTO OUTFILE '/home/dataman/Data/CustomExcerpts/SU_Kindel_IRT_raws/{0}_BrowseEvents_headless.csv'
	FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
	LINES TERMINATED BY '\n'
FROM Edx.EdxTrackEvent ex
WHERE course_display_name = '{1}'
AND event_source = 'server'
AND event_type LIKE '%problem_get%'
ORDER BY `time` ASC
