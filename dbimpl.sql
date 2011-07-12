
create table feeds (
  id serial primary key,
  title varchar(100),
  xmlurl varchar(200) not null,
  htmlurl varchar(200),
  error varchar(100),
  time_to_wait int not null,
  last_read timestamp,
  last_error timestamp
);

create table posts (
  id serial primary key,
  title varchar(100) not null,
  link varchar(200) not null,
  descr text not null,
  pubdate timestamp not null,
  author varchar(100) not null,
  feed int not null
);

create table subscriptions (
  feed int not null,
  username varchar(20) not null
);

create table rated_posts (
  username varchar(20) not null,
  post int not null,
  feed int not null,
  points float not null,
  last_recalc timestamp not null
);