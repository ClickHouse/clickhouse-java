create table article_view_events (
postId Nullable(Float64),
viewTime DateTime DEFAULT now(),
clientId String DEFAULT 'unknown',
address Nested(street String, city String)
) engine = MergeTree order by ();