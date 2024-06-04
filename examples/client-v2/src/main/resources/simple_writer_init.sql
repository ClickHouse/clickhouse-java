create table hacker_news_articles (
id Nullable(Float64),
deleted Nullable(Float64),
type Nullable(String),
by Nullable(String),
time Nullable(String),
text Nullable(String),
dead Nullable(Float64),
parent Nullable(Float64),
poll Nullable(Float64),
kids Array(Nullable(Float64)),
url Nullable(String),
score Nullable(Float64),
title Nullable(String),
parts Array(Nullable(Float64)),
descendants Nullable(Float64)
) engine = MergeTree order by ();