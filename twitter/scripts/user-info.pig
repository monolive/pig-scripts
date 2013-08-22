/*

Extract information about user
 - How many friends
 - How many followers
 - ...

*/
REGISTER '/home/orenault/pig-scripts/twitter/elephant-birds-jar/elephant-bird-hadoop-compat-4.1.jar';
REGISTER '/home/orenault/pig-scripts/twitter/elephant-birds-jar/json-simple-1.1.jar';
REGISTER '/home/orenault/pig-scripts/twitter/elephant-birds-jar/elephant-bird-pig-4.1.jar';
REGISTER '/home/orenault/pig-scripts/twitter/elephant-birds-jar/elephant-bird-core-4.1.jar';

--- Load tweets
raw = load 'tweets/08-Aug-2013/*' using com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') as tweet;

--- extract entities
users = FOREACH raw GENERATE tweet#'user' as user_info;
user_info = FOREACH users GENERATE user_info#'id' as id:long, user_info#'screen_name' as screen_name:chararray, user_info#'friends_count' as friends_count:int, user_info#'followers_count' as followers_count:int, user_info#'profile_image_url' as image;
group_user = GROUP user_info BY (screen_name, id, friends_count, followers_count, image);
count_user = FOREACH group_user GENERATE group as (screen_name, id, friends_count, followers_count, image), COUNT(user_info) as user_count;
order_user = ORDER count_user BY user_count DESC;
--- store order_user into 'tweet_daily_user' using org.apache.hcatalog.pig.HCatStorer('date=20130808');
