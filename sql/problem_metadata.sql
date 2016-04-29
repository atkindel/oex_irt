SELECT 'problem_id', 'problem_display_name', 'course_display_name', 'problem_text', 'trackevent_hook', 'vertical_uri', 'problem_idx', 'sequential_uri', 'vertical_idx'
UNION ALL
SELECT ep.problem_id, ep.problem_display_name, ep.course_display_name, ep.problem_text, ep.trackevent_hook, ep.vertical_uri, ep.problem_idx, ep.sequential_uri, ep.vertical_idx
INTO OUTFILE '/home/dataman/Data/CustomExcerpts/SU_Kindel_IRT_raws/{0}_ProblemMetadata.csv'
	FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
	LINES TERMINATED BY '\n'
FROM Edx.EdxProblem ep
WHERE course_display_name = '{1}'
