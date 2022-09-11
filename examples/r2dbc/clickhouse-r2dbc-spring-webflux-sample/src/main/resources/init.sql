create database clickdb;

create table if not exists clickdb.clicks
(
    domain String,
    path String,
    cdate DateTime,
    count UInt64
)
engine = SummingMergeTree(count)
order by (domain, path, cdate);