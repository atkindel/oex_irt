#!/usr/bin/env bash
for file in ~/Code/irt/data/raws/*_BrowseEvents_headless.csv; do
  outf=$(echo "$file" | sed 's/_headless//g')
  { echo '"event_id","event_type","anon_screen_name","time"'; cat $file; } > $outf
done
rm ~/Code/irt/data/raws/*_BrowseEvents_headless.csv
