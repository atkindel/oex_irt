SELECT EdxPrivate.idInt2Anon(user_id) as anon_screen_name, course_id as course_display_name, grade, unix_timestamp(created_date), (download_url != "") as certificate_granted
INTO OUTFILE '/home/dataman/Data/CustomExcerpts/SU_Kindel_IRT_raws/{0}_Certificates_headless.csv'
	FIELDS ESCAPED BY '"' TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
	LINES TERMINATED BY '\n'
FROM edxprod.certificates_generatedcertificate
WHERE course_id = '{1}'
ORDER BY grade DESC;
