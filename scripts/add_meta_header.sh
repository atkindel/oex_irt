#!/usr/bin/env bash
for file in ~/Code/irt/data/raws/*_ProblemMetadata_headless.csv; do
  outf=$(echo "$file" | sed 's/_headless//g')
  { echo '"problem_id","problem_display_name","course_display_name","problem_text","trackevent_hook","vertical_uri", "problem_idx","sequential_uri","vertical_idx","chapter_uri","sequential_idx","chapter_idx","staff_only","context"'; cat $file; } > $outf
done
rm ~/Code/irt/data/raws/*_ProblemMetadata_headless.csv
