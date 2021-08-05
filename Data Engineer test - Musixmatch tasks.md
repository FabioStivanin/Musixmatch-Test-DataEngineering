# How many devices are active daily? What product? In which countries? With what applications?
essendo questa query basata sui device, tolgo quelli '-' in modod da abbassare il num di righe
(suppongo che se cè un user cè per forza un device, viceversa non sempre vero)

create table testbi.views_custom as (
select trim(guid) as device
, trim(country_code) as country
, CAST(from_unixtime(dtimestamp, '%Y%m%d') as UNSIGNED) as DAY
, trim(product_type) as product
, cast(api_application_id as UNSIGNED) as application
, trim(user_id) as user
from views 
)
;

#indice su device 
create index idx_device
on views_custom(device(136));

#indice su user_id per il prossimo join
create index idx_user_id2
on views_custom(user(36)); #36 max lenght in tab

create index idx_day
on views_custom(DAY); #dimezza il tempo


create table testbi.views_devices as (
select trim(guid) as device
, trim(country_code) as country
, CAST(from_unixtime(dtimestamp, '%Y%m%d') as UNSIGNED) as day
, trim(product_type) as product
, cast(api_application_id as UNSIGNED) as application
from views 
where trim(guid) is not null and trim(guid) <> '-'
)
;
--15Milioni rispetto 31 M di prima (meno della metà)

#indice su device 
create index idx_device3
on views_custom(device(136));

create index idx_day3
on views_custom(DAY); #dimezza il tempo

create index idx_product3
on views_devices(product(19))
;
create index idx_country3
on views_devices(country(2)); 

create index idx_app3
on views_devices(application);


# How many users are active daily? What product? In which countries? With what applications?

come sopra join con users per vedere active/application/country
mi creo una tabella con la distinct degli utenti attivi in modo da non dover andare in join su tutta la user (vista? si può gestire in maniera dinamica? meglio solo mettere indici su user_id e active=1?)

create table active_users as (
select distinct trim(user_id)
from user
where active ='1'
)

-- indice su user_id per il join con views
create index idx_user
on active_users(user_id(36));


ccreate table views_active_users as (
select distinct DAY, user, application, country, product
from views_custom as a
join active_users as b
on a.user=b.user_id
where a.user is not null and a.user <> '-'
)
;

select count(*) from views_active_users; --ridotte le righe a 700mila

create index idx_dayx
on views_active_users(day); 

adesso possiamo farci tutte le group by o count distinct che vogliamo


# What are the most viewed daily lyrics by country and / or application



create table testbi.views_lyrics_count as (
select CAST(from_unixtime(dtimestamp, '%Y%m%d') as UNSIGNED) as day
, cast(api_application_id as UNSIGNED) as application
, trim(country_code) as country
, cast(abstract_id as unsigned) as abstract_id
from views 
where trim(product_type)= 'lyrics' # anche lyrics-restricted ( basta in caso aggiungerlo al filtro)
)
;
10Milioni di righe 


create index idx_day4
on views_lyrics(DAY); #dimezza il tempo

create index idx_country4
on views_lyrics(country(2)); 

create index idx_app4
on views_lyrics(application); 


create table views_lyrics_count as (
select
day, country, application, abstarct_id, count(1) as count_views
from views_lyrics
group by day, country, application, abstarct_id
);
-- 10minuti ma poi basta fare la sum per il count
quasi 8M righe ora

es.
select day, country, abstract_id, sum(count_views) as conto
 from  views_lyrics_count
 where day= 20160101
 group by day, country, abstract_id
 order by day asc, country asc , sum(count_views) desc
 
 ;

create index idx_dayy
on views_lyrics_count(day); 



# How many distinct lyrics account daily, weekly and monthly for 50% of the views. Drill down by country and application. Is this constant over the full period of time

FLOOR(day/100) --mese
FLOOR(day/10000) --anno