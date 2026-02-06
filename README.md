# AmazonBucketToSQL
Scrape urls in Amazon open buckets, and save the data to an SQL .db file

The goal of this script is to capture open bucket urls in a clean and optimized way. Storing the urls in an SQL file also makes it easier to sort and filter for specific data.

# Features
- Resume url scrapes if the script is interrupted
- Start the scrape with a marker or prefix
- Autocheck if the bucket has only the first page exposed, and end the scrape if so
- Export urls to a txt file, seperated by a specific url count
- Export urls of a specific filter
