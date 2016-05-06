SELECT 'course_display_name', 'course_catalog_name', 'academic_year', 'quarter', 'total_enrollment', 'self_paced',
  'start_date', 'enrollment_start', 'end_date', 'enrollment_end', 'grade_policy', 'certs_policy'
UNION ALL
SELECT course_display_name, course_catalog_name, academic_year, quarter, enrollment(course_display_name) as enrolled,
  (course_display_name LIKE "%self%") as sp, start_date, enrollment_start, end_date, enrollment_end,
  grade_policy, certs_policy
INTO OUTFILE '/home/dataman/Data/CustomExcerpts/SU_Kindel_IRT_raws/{0}_CourseInfo.csv'
	FIELDS ESCAPED BY '"' TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
	LINES TERMINATED BY '\n'
FROM Edx.CourseInfo
WHERE course_display_name = '{1}'
