/*

Get 10 most influencal tweets

*/
REGISTER '/home/orenault/pig-scripts/twitter/elephant-birds-jar/elephant-bird-hadoop-compat-4.1.jar';
REGISTER '/home/orenault/pig-scripts/twitter/elephant-birds-jar/json-simple-1.1.jar';
REGISTER '/home/orenault/pig-scripts/twitter/elephant-birds-jar/elephant-bird-pig-4.1.jar';
REGISTER '/home/orenault/pig-scripts/twitter/elephant-birds-jar/elephant-bird-core-4.1.jar';

--- Load Nested JSON
raw = load 'tweets/{08,09}-Aug-2013/*' using com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') as tweet;
describe raw;
--- tweet_extract = FOREACH raw GENERATE tweet#'text' as text:chararray, tweet#'retweet_count' as retweet_count:chararray, tweet#'retweeted' as retweet:boolean;
tweet_extract = FOREACH raw GENERATE (chararray)tweet#'text' as text, (int)tweet#'retweet_count' as retweet_count, (boolean)tweet#'retweeted' as retweet;
--- tweet_extract_conv = FOREACH tweet_extract GENERATE (chararray)text, (chararray)retweet_count, (boolean)retweet;
--- dump tweet_extract;
retweet_only = FILTER tweet_extract BY ( retweet_count != 0 );
dump retweet_only;




/*
retweet = FILTER 


--- extract relevant info from tweet
clean = FOREACH raw GENERATE (chararray)tweet#'text' as text, (long)tweet#'id' as id, (boolean)tweet#'favorited' as favorite;

*/
/*
dump clean;
small = LIMIT clean 10;
dump small
*/
--- tweet_favorited = FILTER clean BY favorite == FALSE;
--- tweet_favorited = FILTER clean BY favorite matches 'false';
--- dump tweet_favorited;


/*
--- extract text, tweet id and user map from tweets
aa = FOREACH raw generate (chararray)tweet#'text' as text, (long)tweet#'id' as id, com.twitter.elephantbird.pig.piggybank.JsonStringToMap(tweet#'user') as user; 

--- keep text, tweet id from previous extract and extract username, follower and uid out of tweets  
bb = foreach aa generate text,id,user#'screen_name' as name:chararray, user#'followers_count' as follower:int, user#'id' as uid:long;

--- Find most active tweeter, we're going to first group by username and then count how many tweet we've got for each user
group_user = GROUP bb BY name;
active_user = FOREACH group_user GENERATE group as name,  COUNT(bb) as tweet_cnt_user;

--- With ordering to get the 10 most active user
order_active_user = ORDER active_user BY tweet_cnt_user DESC;
first_10_user = LIMIT order_active_user 10;
dump first_10_user;
*/
