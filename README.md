# Data ETL and Schema using Spark to Conduct Song Play Analysis

### Background / Pupose
This projects performs ETL on data generated by a mock startup called Sparkify who provides a music streaming application. Event logs related to song play from users and meta data about the songs are stored in JSON files in AWS S3 which are then extracted, loaded and transformed using Spark and then stored back in AWS S3 in parquet file format.


### Rational
By transforming data using Spark, we are able to process large amounts of data.

The database schema is normalized using a *star schema* and tables are structured in the following format. A star schema reduces data redundancy while also optimizing for specific queries. 

**Fact Table**
- **song_plays:** start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

**Dimension Tables**
- **songs:** song_id, title, artist_id, year, duration
- **artists:** artist_id, name, location, latitude, longitude
- **users:** user_id, firstname, lastname, gender, level
- **times:** start_time, hour, day, week, month, year, weekday

### File Description & Steps to Run ETL
- `dl.cfg` includes parameters required to access S3 resources
- `etl.py` includes script that runs ETL
-  Paste AWS Secret and Key in the `dl.cfg` file
- `s3-etl.ipynb` is used to test etl script.
