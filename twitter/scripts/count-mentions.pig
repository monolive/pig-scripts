/*

Get the 10 user the most mentioned

*/
REGISTER '/home/orenault/pig-scripts/twitter/elephant-birds-jar/elephant-bird-hadoop-compat-4.1.jar';
REGISTER '/home/orenault/pig-scripts/twitter/elephant-birds-jar/json-simple-1.1.jar';
REGISTER '/home/orenault/pig-scripts/twitter/elephant-birds-jar/elephant-bird-pig-4.1.jar';
REGISTER '/home/orenault/pig-scripts/twitter/elephant-birds-jar/elephant-bird-core-4.1.jar';

--- Load tweets
raw = load 'tweets/{08-Aug-2013,09-Aug-2013}/*' using com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') as tweet;

--- extract entities
entities = FOREACH raw GENERATE tweet#'entities' as tweet_entities;
/*
--- HASHTAGS
hashtag_mention = FILTER entities BY (NOT IsEmpty(tweet_entities#'hashtags'));
hashtag_mention = FOREACH hashtag_mention GENERATE FLATTEN((bag{})tweet_entities#'hashtags') as (hash_tags:[]);
hashtags = FOREACH hashtag_mention GENERATE LOWER(hash_tags#'text') as text;
group_by_hashtags = GROUP hashtags BY text;
popular_hashtags = FOREACH group_by_hashtags GENERATE group as text, COUNT(hashtags) as hashtags_counter;
order_hashtags = ORDER popular_hashtags BY hashtags_counter DESC;
most_popular_hashtags = LIMIT order_hashtags 10;
dump most_popular_hashtags;
*/
--- USERMENTION
user_mention = FILTER entities BY (NOT IsEmpty(tweet_entities#'user_mentions'));
user_mention = FOREACH user_mention GENERATE FLATTEN((bag{})tweet_entities#'user_mentions') as (user:[]);
user = FOREACH user_mention GENERATE LOWER(user#'screen_name') as screen_name;

group_by_screename = GROUP user BY screen_name;
popular_screename = FOREACH group_by_screename GENERATE group as screen_name, COUNT(user) as user_counter;
order_screename = ORDER popular_screename BY user_counter DESC;
most_popular_screename = LIMIT order_screename 10;
dump most_popular_screename;
