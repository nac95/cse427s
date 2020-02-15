datatest = LOAD 'test_ad_data.txt' AS (campaign_id:chararray,
             date:chararray, time:chararray,
             keyword:chararray, display_site:chararray, 
             placement:chararray, was_clicked:int, cpc:int);

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

datagroup = GROUP data by keyword;

datatotal = FOREACH datagroup GENERATE group, SUM(datatest.cpc) AS sumcpc;

dataorder = ORDER datatotal BY sumcpc DESC;

datalimit = LIMIT dataorder 3;

DUMP datalimit;
