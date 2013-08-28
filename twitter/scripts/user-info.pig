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

--- extract user information
users = FOREACH raw GENERATE tweet#'user' as user_info;
user_info = FOREACH users GENERATE (long)user_info#'id' as id, (chararray)user_info#'screen_name' as screen_name, (int)user_info#'friends_count' as friends_count, (int)user_info#'followers_count' as followers_count, (chararray)user_info#'profile_image_url' as image;
group_user = GROUP user_info BY (screen_name, id, friends_count, followers_count, image);
describe group_user;
count_user = FOREACH group_user GENERATE group as (screen_name, id, friends_count, followers_count, image), COUNT(user_info) as user_count;
flatten_count_user = FOREACH count_user GENERATE $0.screen_name as screen_name, $0.id as id, $0.friends_count as friends_count, $0.followers_count as followers_count, $0.image as profile_image:chararray, (int)user_count;
order_user = ORDER flatten_count_user BY user_count DESC;
describe order_user;

--- need to look into partition
store order_user into 'tweet_daily_user' using org.apache.hcatalog.pig.HCatStorer();
