/*

Count all individual hashtag

*/
REGISTER '/home/orenault/pig-scripts/twitter/elephant-birds-jar/elephant-bird-hadoop-compat-4.1.jar';
REGISTER '/home/orenault/pig-scripts/twitter/elephant-birds-jar/json-simple-1.1.jar';
REGISTER '/home/orenault/pig-scripts/twitter/elephant-birds-jar/elephant-bird-pig-4.1.jar';
REGISTER '/home/orenault/pig-scripts/twitter/elephant-birds-jar/elephant-bird-core-4.1.jar';

--- Load tweets
raw = load 'tweets/{08-Aug-2013,09-Aug-2013}/*' using com.twitter.elephantbird.pig.load.JsonLoader() as tweet;

--- extract text, tweet id and user map from tweets
clean1 = FOREACH raw generate (chararray)tweet#'text' as text, (long)tweet#'id' as id, com.twitter.elephantbird.pig.piggybank.JsonStringToMap(tweet#'user') as user; 

--- keep text, tweet id from previous extract and extract username, follower and uid out of tweets  
clean2 = foreach clean1 generate REGEX_EXTRACT_ALL(text,''),id,user#'screen_name' as name:chararray, user#'followers_count' as follower:int, user#'id' as uid:long;

--- Find most active tweeter, we're going to first group by username and then count how many tweet we've got for each user
group_user = GROUP clean2 BY name;
active_user = FOREACH group_user GENERATE group as name,  COUNT(bb) as tweet_cnt_user;

--- With ordering to get the 10 most active user
order_active_user = ORDER active_user BY tweet_cnt_user DESC;
first_10_user = LIMIT order_active_user 10;
dump first_10_user;
