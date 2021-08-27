%python

driver = "org.mariadb.jdbc.Driver"
url = "jdbc:mysql://mxm-testbi.c72srgqwk8ib.us-east-1.rds.amazonaws.com:3306/testbi"
user = "mxmtest"
password = "LDX6uq26J4JTqWzm"

# TASK 1

table= "views_devices"

device_table = spark.read.format("jdbc")\
  .option("driver", driver)\
  .option("url", url)\
  .option("dbtable", table)\
  .option("user", user)\
  .option("password", password)\
  .load()

device_table.createOrReplaceTempView("DEVICES")
# ALTERNATIVA
device_table.write.saveAsTable("DEVICES")
DROP TABLE IF EXISTS <table>     // deletes the metadata and data
CREATE TABLE <table> AS SELECT ...

# DELTA
device_table.write.format("delta").save("/delta/devices")

%sql
  
select count(distinct device) as count_devices
, a.day
  from DEVICES a
where day = 20160101 
  --floor(day/100) = 201601
  group by a.day
  order by count_devices desc
  limit 10
 ; 

%sql
  
select count(distinct device) as count_devices
, a.day
, product
, country
 from DEVICES a
where day= 20160131
  group by  a.day, product, country
  order by count_devices desc
  limit 10


# TASK 2

%python
  
table= "views_active_users"

user_table = spark.read.format("jdbc")\
  .option("driver", driver)\
  .option("url", url)\
  .option("dbtable", table)\
  .option("user", user)\
  .option("password", password)\
  .load()

user_table.createOrReplaceTempView("USERS")

%sql
  
select count(distinct user) as count_users
, a.country
, a.application
, a.product
, a.day
  from USERS a 
  group by  a.country
, a.application
, a.product
, a.day
  order by count_users desc


  # TASK 3

%python
  
table= "views_lyrics_count"

lyrics_table = spark.read.format("jdbc")\
  .option("driver", driver)\
  .option("url", url)\
  .option("dbtable", table)\
  .option("user", user)\
  .option("password", password)\
  .load()

lyrics_table.createOrReplaceTempView("LYRICS")


%sql
  
select  rank() over (partition by application, country  order by sum(count_views) desc ) as ranking
, country, application, abstract_id
, sum(count_views) as views
from  LYRICS a
where  day = 20160101
group by abstract_id, country, application


# TASK 4

%sql

with ranking as (
SELECT abstract_id, sum(count_views) as views
,row_number() over (order by sum(count_views) desc) as ranking
FROM LYRICS
where 
day=20160101 --daily
--day between 20160101 and 20160107 --weekly
--floor(day/100) = 201601 --monthly
group by abstract_id
)

, total as (
SELECT sum(count_views) as tot_views
FROM LYRICS
where 
day=20160101 --daily
--day between 20160101 and 20160107 --weekly
--floor(day/100) = 201601 --monthly
)

, cumulative_views as (
select abstract_id, views, ranking
, SUM(views) 
          over ( ORDER BY ranking ROWS BETWEEN unbounded preceding AND CURRENT ROW ) as cumsum 
from ranking
order by cumsum 
)

, percentual as (
select a.abstract_id, views, ranking, cumsum, tot_views
, cumsum/tot_views * 100 as perc 
from cumulative_views a
, total b

)

select min(ranking) as count_fifty_perc_views
from percentual
where perc >= 50





#TREND

%sql

with ranking as (
SELECT abstract_id, day, sum(count_views) as views
,row_number() over (partition by day order by sum(count_views) desc) as ranking
FROM LYRICS
where 
--day=20160101 --daily
--day between 20160101 and 20160107 --weekly
floor(day/100) = 201601 --monthly
group by abstract_id, day
)

, total as (
SELECT sum(count_views) as tot_views
, day 
FROM LYRICS
where 
--day=20160101 --daily
--day between 20160101 and 20160107 --weekly
floor(day/100) = 201601 --monthly
group by day
)

, cumulative_views as (
select abstract_id, views, ranking, day
, SUM(views) 
          over (partition by day ORDER BY ranking ROWS BETWEEN unbounded preceding AND CURRENT ROW ) as cumsum 
from ranking
order by cumsum 
)

, percentage as (
select a.abstract_id, views, ranking, cumsum, tot_views, a.day
, cumsum/tot_views * 100 as perc 
from cumulative_views a
join total b
on a.day=b.day
)

select min(ranking) as minimo
, substring(day, 7,2) as day
from percentage
where perc >= 50
group by substring(day, 7,2)

# DRILL DOWN


%sql

with ranking as (
SELECT abstract_id, day, country, application, sum(count_views) as views
,row_number() over (partition by day, country, application order by sum(count_views) desc) as ranking
FROM LYRICS
where 
day=20160101 --daily
--day between 20160101 and 20160107 --weekly
--floor(day/100) = 201601 --monthly
group by abstract_id, day, country, application
)

, total as (
SELECT sum(count_views) as tot_views
, day, country, application
FROM LYRICS
where 
day=20160101 --daily
--day between 20160101 and 20160107 --weekly
--floor(day/100) = 201601 --monthly
group by day, country, application
)

, cumulative_views as (
select abstract_id, views, ranking, day, country, application
, SUM(views) 
          over (partition by day, country, application ORDER BY ranking ROWS BETWEEN unbounded preceding AND CURRENT ROW ) as cumsum 
from ranking
order by cumsum 
)

, percentage as (
select a.abstract_id, views, ranking, cumsum, tot_views, a.day, a.country, a.application
, cumsum/tot_views * 100 as perc 
from cumulative_views a
join total b
on a.day=b.day
and a.country = b.country
and a.application = b.application
)

select day, country, application
, min(ranking) as fifty_perc_count
from percentage
where perc >= 50
group by day, country, application
order by fifty_perc_count desc




