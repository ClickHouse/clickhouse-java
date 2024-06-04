create table article_view_events (
postId Nullable(Float64),
viewTime DateTime DEFAULT now(),
clientId String DEFAULT 'unknown',
) engine = MergeTree order by ();