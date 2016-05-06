SELECT 'event_type', 'resource_display_name', 'video_current_time', 'video_speed', 'video_new_speed', 'video_old_speed',
  'video_new_time', 'video_old_time', 'video_seek_type', 'video_codec', 'time', 'course_display_name', 'quarter',
  'anon_screen_name', 'video_id'
UNION ALL
SELECT *
INTO OUTFILE '/home/dataman/Data/CustomExcerpts/SU_Kindel_IRT_raws/{0}_VideoEvents.csv'
	FIELDS ESCAPED BY '"' TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
	LINES TERMINATED BY '\n'
FROM Edx.VideoInteraction
WHERE course_display_name = '{1}'
