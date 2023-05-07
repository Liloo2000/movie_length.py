/*
Line number example
*/

-- load input from text files in directory
lines = LOAD '/root/input/' AS (line:chararray);

lines_count = FOREACH lines GENERATE COUNT(line) as cnt;

-- order by count
line_count_sorted = ORDER lines_count BY cnt DESC;

-- limit number of results
line_count_sorted_limit = LIMIT line_count_sorted 10;

-- print results
DUMP line_count_sorted_limit;
