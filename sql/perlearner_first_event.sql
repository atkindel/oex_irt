SELECT 'anon_screen_name', 'course_display_name', 'enrolled'
UNION ALL
SELECT edxprod.idInt2Anon(user_id), course_id, unix_timestamp(created)
INTO OUTFILE '/home/dataman/Data/CustomExcerpts/SU_Kindel_IRT_raws/{0}_Registrations.csv'
	FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
	LINES TERMINATED BY '\n'
FROM edxprod.student_courseenrollment
WHERE course_id = '{1}'
