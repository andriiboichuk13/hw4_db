create or replace table steam as (
    with games as (
       select unnest(games) as g
        from read_json(
        'C:/Users/aboic/Downloads/steam_2025_5k-dataset-games_20250831.json',
        maximum_object_size = 2147000000)
    ),
    genres as (
       select g.appid,
              genre.description as game_genre
        from games g,
        unnest(g.app_details.data.genres) AS t(genre)
    ),
    categories as (
       select g.appid,
              categories.description as category
        from games g,
        unnest(g.app_details.data.categories) AS t(categories)
    )
    select g.appid,
            g.name_from_applist,
            g.app_details.data.required_age,
            g.app_details.data.is_free,
            g.app_details.data.type,
            g.app_details.data.release_date.date,
            coalesce(g.app_details.data.price_overview.currency, 'free/to be released') as currency,
            case when g.app_details.data.is_free = true then 0
            else (g.app_details.data.price_overview.final)/100
            end as game_price,
            g.app_details.data.platforms.windows,
            g.app_details.data.platforms.mac,
            g.app_details.data.platforms.linux,
            g1.game_genre,
            c.category
    from games g
    left join genres g1 on g.appid = g1.appid
    left join categories c on g.appid = c.appid
    );

alter table steam alter column required_age type int32;

create or replace table steam2 as (
    with games as (
        select unnest(games) as g
        from read_json(
                'C:/Users/aboic/Downloads/steam_2025_5k-dataset-games_20250831.json',
                maximum_object_size = 2147000000)
    ),
    developers as (
       select g.appid,
              developer
        from games g,
        unnest(g.app_details.data.developers) as t(developer)
    ),
    publishers as (
       select g.appid,
              publisher
       from games g,
       unnest(g.app_details.data.publishers) as t(publisher)
    )
    select s.*,
           d.developer,
           p.publisher
           from steam s
    left join developers d on s.appid = d.appid
    left join publishers p on s.appid = p.appid
);



with only_first_instance as (
       select
       s.appid,
       s.game_genre,
       s.game_price,
       s.currency,
       s.is_free,
       s.windows,
       s.mac,
       s.linux
       from steam2 s
       where s.currency = 'USD' or (s.currency = 'free/to be released' and s.is_free = true)
),
distinct_rows as (
    select distinct *
    from only_first_instance
)
select
    o.game_genre,
    count(distinct(o.appid)) as count_by_genre,
    round(avg(o.game_price), 2) as average_price_by_genre,
    round(100*avg(cast(o.is_free as int)), 2) as percentage_of_free_games,
    round(100*avg(cast(o.windows as int)), 2) as percentage_of_windows_availability,
    round(100*avg(cast(o.mac as int)), 2) as percentage_of_mac_availability,
    round(100*avg(cast(o.linux as int)), 2) as percentage_of_linux_availability
    from distinct_rows o
    group by o.game_genre
    order by average_price_by_genre desc;



with age_gap as (
    select s.appid,
           s.game_genre,
           s.required_age,
           case when s.required_age < 12 then 'child (0-11)'
            when s.required_age >11 and s.required_age < 15 then 'early teen (12-14)'
            when s.required_age >14 and s.required_age <18 then 'late teen(15-17)'
            when s.required_age > 17 and s.required_age < 21 then 'early adult(18-20)'
           else 'fully-grown adult(21+)'
           end as age_group,
          case when s.required_age < 12 then 1
               when s.required_age >11 and s.required_age < 15 then 2
               when s.required_age >14 and s.required_age <18 then 3
               when s.required_age > 17 and s.required_age < 21 then 4
               else 5
               end as order_category
    from steam2 s
),
distinct_age_gap as (
    select distinct * from age_gap
)
select order_category,
       age_group,
       round(100*count(*) filter (where game_genre in('Экшены', 'Sexual Content', 'Gore', 'Action', 'Nudity'))/count(*), 4) as percentage_of_non_childish_games,
       count(*) filter (where game_genre in('Экшены', 'Sexual Content', 'Gore', 'Action', 'Nudity')) as count_non_child_friendly_genres,
       count(*) as count_all_genres
from distinct_age_gap
    group by age_group, order_category
    order by order_category;




create or replace table steam2 as (
       select *,
       try(year(strptime(s.date, '%b %d, %Y'))) as year_of_release
       from steam2 s
       );


with only_first_instance as (
    select
        s.appid,
        s.game_genre,
        s.game_price,
        s.currency,
        s.year_of_release
    from steam2 s
    where (s.currency = 'USD' or (s.currency = 'free/to be released' and s.is_free = true)) and s.year_of_release is not null and s.game_genre is not null
),
     distinct_rows as (
         select distinct *
         from only_first_instance
     )
select
    o.year_of_release,
    o.game_genre,
    count(distinct(o.appid)) as count_by_genre,
    round(avg(o.game_price), 2) as average_price_by_genre
from distinct_rows o
group by o.year_of_release, o.game_genre
order by year_of_release asc, average_price_by_genre desc;




with only_first_instance as (
    select s.appid,
           s.game_genre,
            s.game_price,
            s.currency,
            s.developer
     from steam2 s
     where developer is not null and currency in ('USD', 'free/to be released')
     ),
    distinct_instance as (
        select distinct * from only_first_instance
    )
    select developer,
           count(distinct appid) as count_app,
           round(avg(game_price),2) as average_price,
           max(game_price) as max_price,
           min(game_price) as min_price
            from distinct_instance d
            group by developer
            order by count_app desc;



with only_first_instance as (
    select s.appid,
           s.game_genre,
           s.game_price,
           s.currency,
           s.developer,
           s.publisher
    from steam2 s
    where developer is not null and publisher is not null and currency in ('USD', 'free/to be released') and s.developer = s.publisher
),
     distinct_instance as (
         select distinct * from only_first_instance
     )
select developer, publisher,
       count(distinct appid) as count_app,
       round(avg(game_price),2) as average_price,
       max(game_price) as max_price,
       min(game_price) as min_price
from distinct_instance d
group by developer, publisher
order by count_app desc;


