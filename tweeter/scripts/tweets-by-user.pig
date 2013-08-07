/*

Extract text and user information from tweets

*/
REGISTER '/home/orenault/pig-scripts/tweeter/elephant-birds-jar/elephant-bird-hadoop-compat-4.1.jar';
REGISTER '/home/orenault/pig-scripts/tweeter/elephant-birds-jar/json-simple-1.1.jar';
REGISTER '/home/orenault/pig-scripts/tweeter/elephant-birds-jar/elephant-bird-pig-4.1.jar';
REGISTER '/home/orenault/pig-scripts/tweeter/elephant-birds-jar/elephant-bird-core-4.1.jar';

--- Load Nested JSON
raw = load 'tweets/30-Jul-2013/*' using com.twitter.elephantbird.pig.load.JsonLoader() as tweet;
/* Structure of raw
([filter_level#medium,contributors#,text#BT chief steps down to take UK government role as Sky Sports battle intensifies http://t.co/DmDEE2eLpC,geo#,retweeted#false,in_reply_to_screen_name#,possibly_sensitive#false,truncated#false,lang#en,entities#{"symbols":[],"urls":[{"expanded_url":"http:\/\/tnw.to\/h0fxP","indices":[80,102],"display_url":"tnw.to\/h0fxP","url":"http:\/\/t.co\/DmDEE2eLpC"}],"hashtags":[],"user_mentions":[]},in_reply_to_status_id_str#,id#347318524840136704,source#<a href="http://spread.us" rel="nofollow">Spread The Next Web</a>,in_reply_to_user_id_str#,favorited#false,in_reply_to_status_id#,retweet_count#0,created_at#Wed Jun 19 11:42:43 +0000 2013,in_reply_to_user_id#,favorite_count#0,id_str#347318524840136704,place#,user#{"location":"Dublin, Ireland, EU","default_profile":false,"profile_background_tile":false,"statuses_count":26174,"lang":"en","profile_link_color":"0084B4","profile_banner_url":"https:\/\/pbs.twimg.com\/profile_banners\/30942383\/1360064327","id":30942383,"following":null,"protected":false,"favourites_count":42,"profile_text_color":"333333","description":"Founder http:\/\/iMusician.Org & http:\/\/UrbanUnsigned.com @iMusicianMedia & @UUMagazine","verified":false,"contributors_enabled":false,"profile_sidebar_border_color":"000000","name":"Gregory Donaghy","profile_background_color":"000000","created_at":"Mon Apr 13 19:51:36 +0000 2009","default_profile_image":false,"followers_count":523,"profile_image_url_https":"https:\/\/si0.twimg.com\/profile_images\/1179957159\/IMG_0073_normal.jpg","geo_enabled":true,"profile_background_image_url":"http:\/\/a0.twimg.com\/profile_background_images\/782703188\/9eabf48b297cfd09e6f4e9b337ffbd25.gif","profile_background_image_url_https":"https:\/\/si0.twimg.com\/profile_background_images\/782703188\/9eabf48b297cfd09e6f4e9b337ffbd25.gif","follow_request_sent":null,"url":"http:\/\/globalmediagarden.com","utc_offset":0,"time_zone":"Dublin","notifications":null,"friends_count":210,"profile_use_background_image":true,"profile_sidebar_fill_color":"C0DFEC","screen_name":"GregoryDonaghy","id_str":"30942383","profile_image_url":"http:\/\/a0.twimg.com\/profile_images\/1179957159\/IMG_0073_normal.jpg","is_translator":false,"listed_count":29},coordinates#])
*/

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

/*
--- Get user with most followers
max_follower = FOREACH group_user GENERATE MAX(follower);
dump max_follower

*/
