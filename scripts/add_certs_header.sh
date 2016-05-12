#!/usr/bin/env bash
for file in ~/Code/irt/data/raws/*_Certificates_headless.csv; do
  outf=$(echo "$file" | sed 's/_headless//g')
  { echo '"anon_screen_name","course_display_name","grade","created_date","certificate_granted"'; cat $file; } > $outf
done
rm ~/Code/irt/data/raws/*_Certificates_headless.csv
