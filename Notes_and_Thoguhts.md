Have gone back and forth on whether to do traditional data engineering pipeline (store each layer - e.g. raw, clean, etc.) or to do transformation right after the fetch call.

Since Drift is the first datasource, I ended up going with the transform after call style.

For binance I think I'll store the raw data and then I can compare the two approaches

The downside to storing the raw data is that certain things such as dedupe become a challenege, also there is the question of rolling vs fixed windows.

DRIFT returns fixed windows, like:
12:01:00 - 12:02:00

But other datasources might get you exact windows down to lowest granularity they support (e.g. second)
so the following windows would have different ohlc:
12:01:00 - 12:02:00
12:01:01 - 12:02:01
12:01:02 - 12:02:02

Maybe an answer is to only ever request exact UTC minute sliced data, like:  2024-11-28 22:12:00+00:00

Maybe storing a hashmap of urls to json responses? and then every response is stored as its own file? 

Thats starting to feel like getting into "db" territory, but really trying to keep this lightweight. 

It's becoming a question of how complete is the system. (The nice thing about the list of URLs is that you can generate beforehand and then the hashmap would dtore the file_uri of the res as the value, so you could simply query the hashmap to see the completeness of the data for a "URL possibility space" perspective)


Lightweight Meaning:
1. user clones qlir, 
2. runs quickstart.py, 
3. opens the project folder
4. runs get_data (or update_data) to fetch data (and persist to disk)
5. loads the data from disk in their project 
6. then filters the df down if they want a smaller window

