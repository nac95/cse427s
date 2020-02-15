-- TODO (A): Replace 'FIXME' to load the test_ad_data.txt file.

datatest = LOAD 'test_ad_data.txt' AS (campaign_id:chararray,
             date:chararray, time:chararray,
             keyword:chararray, display_site:chararray, 
             placement:chararray, was_clicked:int, cpc:int);

dataad_data1 = LOAD '/dualcore/ad_data1.txt' AS (campaign_id:chararray,
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

-- TODO (B): Include only records where was_clicked has a value of 1

datafilter = FILTER data BY was_clicked == 1;

-- TODO (C): Group the data by the appropriate field

databydisplay_site = GROUP datafilter BY display_site;

--/* TODO (D): Create a new relation which includes only the 
 --*           display site and the total cost of all clicks 
 --*           on that site
 --*/

datatotal = FOREACH databydisplay_site GENERATE group, SUM(datafilter.cpc) AS sumcpc;


-- TODO (E): Sort that new relation by cost (ascending)

dataorder = ORDER datatotal BY sumcpc ASC;

 
-- TODO (F): Display just the first three records to the screen

datalimit = LIMIT dataorder 4;

DUMP datalimit;
