SELECT ep.problem_id, ep.problem_display_name, ep.course_display_name, ep.problem_text, ep.trackevent_hook,
 ep.vertical_uri, ep.problem_idx, ep.sequential_uri, ep.vertical_idx, ep.chapter_uri, ep.sequential_idx,
 ep.chapter_idx, ep.staff_only, CONCAT(ep.chapter_idx, '.', ep.sequential_idx, '.', ep.vertical_idx)
INTO OUTFILE '/home/dataman/Data/CustomExcerpts/SU_Kindel_IRT_raws/{0}_ProblemMetadata_headless.csv'
	FIELDS ESCAPED BY '"' TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
	LINES TERMINATED BY '\n'
FROM Edx.EdxProblem ep
WHERE course_display_name = '{1}'
AND ep.chapter_idx > 0
ORDER BY chapter_idx, sequential_idx, vertical_idx, problem_idx
