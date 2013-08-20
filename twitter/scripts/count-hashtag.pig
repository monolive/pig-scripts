/*

Count all individual hashtag

*/
REGISTER '/home/orenault/pig-scripts/twitter/elephant-birds-jar/elephant-bird-hadoop-compat-4.1.jar';
REGISTER '/home/orenault/pig-scripts/twitter/elephant-birds-jar/json-simple-1.1.jar';
REGISTER '/home/orenault/pig-scripts/twitter/elephant-birds-jar/elephant-bird-pig-4.1.jar';
REGISTER '/home/orenault/pig-scripts/twitter/elephant-birds-jar/elephant-bird-core-4.1.jar';

--- Load tweets
raw = load 'tweets/{08-Aug-2013,09-Aug-2013}/*' using com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') as tweet;

--- extract entities
entities = FOREACH raw GENERATE tweet#'entities' as tweet_entities;
hashtag_mention = FILTER entities BY (NOT IsEmpty(tweet_entities#'hashtags'));
--- hashtags = FOREACH hashtag_mention GENERATE FLATTEN((bag{})tweet_entities#'hashtags') as hash_tags;
hashtags = FOREACH hashtag_mention GENERATE FLATTEN((bag{})tweet_entities#'hashtags') as hash_tags;
dump hashtags;
/*
({([indices#{(57),(61)},text#tft])})
({([indices#{(56),(64)},text#bigdata]),([indices#{(65),(75)},text#analytics]),([indices#{(76),(93)},text#rosslynanalytics])})
*/
/*
hashtags = FOREACH hashtag_mention GENERATE tweet_entities#'hashtags' as hash_tags:map(indices:chararray,text:chararray);
hashtag = FOREACH hashtags GENERATE hash_tags#'text';
dump hashtag;
user_mentions = FILTER entities BY (NOT IsEmpty(tweet_entities#'user_mentions'));
dump user_mentions;
clean1 = FOREACH raw GENERATE com.twitter.elephantbird.pig.piggybank.JsonStringToMap(tweet#'entities') as entities, (long)tweet#'id' as id; 
user_ref = FILTER 
small = LIMIT clean1 10;
dump small;
--- keep text, tweet id from previous extract and extract username, follower and uid out of tweets  
clean2 = foreach clean1 generate REGEX_EXTRACT_ALL(text,''),id,user#'screen_name' as name:chararray, user#'followers_count' as follower:int, user#'id' as uid:long;

--- Find most active tweeter, we're going to first group by username and then count how many tweet we've got for each user
group_user = GROUP clean2 BY name;
active_user = FOREACH group_user GENERATE group as name,  COUNT(bb) as tweet_cnt_user;

--- With ordering to get the 10 most active user
order_active_user = ORDER active_user BY tweet_cnt_user DESC;
first_10_user = LIMIT order_active_user 10;
dump first_10_user;
*/
