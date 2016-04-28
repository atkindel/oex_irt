SELECT 'event_id', 'anon_screen_name', 'event_type', 'event_source', 'time', 'resource_display_name', 'page', 'problem_id', 'problem_choice', 'submission_id', 'attempts', 'success', 'answer_id', 'answer'
UNION ALL
SELECT ex.event_id, ex.anon_screen_name, ex.event_type, ex.event_source, ex.time, ex.resource_display_name, ex.page, ex.problem_id, ex.problem_choice, ex.submission_id, ex.attempts, ex.success, ea.answer_id, ea.answer
INTO OUTFILE '/home/dataman/Data/CustomExcerpts/SU_Kindel_IRT_raws/{0}_ProblemEvents.csv'
	FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
	LINES TERMINATED BY '\n'
FROM Edx.EdxTrackEvent ex
LEFT JOIN Edx.Answer ea
	ON ex.answer_fk = ea.answer_id
WHERE course_display_name = '{1}'
AND event_type LIKE '%problem_check%'
AND (char_length(success) > 1 OR event_source = 'browser');
