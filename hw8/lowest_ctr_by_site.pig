-- Load only the ad_data1 and ad_data2 directories
ddataad_data1 = LOAD '/dualcore/ad_data1.txt' AS (campaign_id:chararray,
             date:chararray, time:chararray,
             keyword:chararray, display_site:chararray,
             placement:chararray, was_clicked:int, cpc:int);

dataad_data2 = LOAD '/dualcore/ad_data2.txt' AS (campaign_id:chararray,
             date:chararray, time:chararray,
             keyword:chararray, display_site:chararray,
             placement:chararray, was_clicked:int, cpc:int);

dataad1filter = FILTER dataad_data1 BY county == 'USA';
dataad1 = FOREACH dataad1filter GENERATE campaign_id, date, time, TRIM(UPPER(keyword)), display_site, placement, was_clicked, cpc;

dataad2 = FOREACH dataad_data2 GENERATE campaign_id, date, time, TRIM(UPPER(keyword)), display_site, placement, was_clicked, cpc;

data = UNION dataad1, dataad2;

grouped = GROUP data BY display_site;


by_site = FOREACH grouped {
  -- Include only records where the ad was clicked
datafilter = FILTER data BY was_clicked == 1;
  -- count the number of records in this group
datacount = COUNT(datafilter);
  --/* Calculate the click-through rate by dividing the 
   --* clicked ads in this group by the total number of ads
   --* in this group.
   --*/
counttotal = COUNT(data.was_clicked);
div = (datacount/counttotal)*(long)100;
ctr = CONCAT((chararray)div, '%');
GENERATE group, ctr AS ctr;
}

-- sort the records in ascending order of clickthrough rate
dataorder = ORDER by_site BY ctr ASC;

-- show just the first three
datalimit = LIMIT dataorder 4;

DUMP datalimit;
