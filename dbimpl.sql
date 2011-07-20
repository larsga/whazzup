
create table feeds (
  id serial primary key,
  title varchar(100),
  xmlurl varchar(200) not null,
  htmlurl varchar(200),
  error varchar(100),
  time_to_wait int not null,
  last_read timestamp,
  last_error timestamp,
  max_posts int
);

create table posts (
  id serial primary key,
  title varchar(200) not null, -- at what point do we truncate?
  link varchar(400) not null,
  descr text not null,
  pubdate timestamp not null,
  author varchar(100),
  feed int not null
);

create table subscriptions (
  feed int not null,
  username varchar(20) not null,
  up int not null default 0,
  down int not null default 0,
  PRIMARY KEY (feed, username)
);

create table rated_posts (
  username varchar(20) not null,
  post int not null,
  feed int not null,
  points float not null,
  last_recalc timestamp not null,
  prob float not null,
  PRIMARY KEY (username, post)
);
create index on rated_posts_storylist on rated_posts (username, points);

create table read_posts (
  username varchar(20) not null,
  post int not null,
  feed int not null,
  PRIMARY KEY (username, post)
);

create table users (
  username varchar(20) not null primary key,
  password varchar(32) not null,
  email varchar(100) not null
);

create table notify (
  email varchar(100) not null primary key
)

-- ORIGINAL QUERY PLAN
--
-- Limit  (cost=121.85..121.91 rows=25 width=474)
--   ->  Sort  (cost=121.85..123.46 rows=643 width=474)
--         Sort Key: rated_posts.points
--         ->  Hash Join  (cost=25.08..103.70 rows=643 width=474)
--               Hash Cond: (p.id = rated_posts.post)
--               ->  Seq Scan on posts p  (cost=0.00..68.60 rows=960 width=466)
--               ->  Hash  (cost=17.04..17.04 rows=643 width=12)
--                     ->  Seq Scan on rated_posts  (cost=0.00..17.04 rows=643 width=12)
--                           Filter: ((username)::text = 'larsga'::text)

-- ADDED PRIMARY KEY
--
-- Limit  (cost=133.55..133.61 rows=25 width=474)
--   ->  Sort  (cost=133.55..135.62 rows=828 width=474)
--         Sort Key: rated_posts.points
--         ->  Hash Join  (cost=29.70..110.18 rows=828 width=474)
--               Hash Cond: (p.id = rated_posts.post)
--               ->  Seq Scan on posts p  (cost=0.00..68.60 rows=960 width=466)
--               ->  Hash  (cost=19.35..19.35 rows=828 width=12)
--                     ->  Seq Scan on rated_posts  (cost=0.00..19.35 rows=828 width=12)
--                           Filter: ((username)::text = 'larsga'::text)

-- ADDED STORYLIST INDEX
--
-- Limit  (cost=0.00..16.58 rows=25 width=474)
--   ->  Nested Loop  (cost=0.00..549.25 rows=828 width=474)
--         ->  Index Scan Backward using rated_posts_storylist on rated_posts  (cost=0.00..61.24 rows=828 width=12)
--               Index Cond: ((username)::text = 'larsga'::text)
--         ->  Index Scan using posts_pkey on posts p  (cost=0.00..0.58 rows=1 width=466)
--               Index Cond: (p.id = rated_posts.post)
