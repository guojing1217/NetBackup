#~/bin/bash
grep ^D avail.out|awk '{ SUM += $8} END { print SUM/1024/1024/1024 }'
