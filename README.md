## Udacity Data Engineering - Data Lakes with Spark

The project aims to create an ETL pipeline for sparkify, a music streaming application. Currently, log files and song data is kept in s3 in json files. The project will use spark processing, to read the log files from S3, process the data to extract the songs, artists, users, times and songplays, and then load back into S3 in a set of dimensional tables. This enables the analytics team to perform queries on the extracted data to further understand the use of the streaming application.

## Running the Project

To run the project, AWS access keys must be supplied in the form given in 'df.cfg.example'. The keys provided must have read, and write access to AWS S3.

To run the project locally, without using the AWS drivres, specify 'USE_AWS=false', in the configuration.

## Process

The ETL pipeline is defined in 'etl.py'.

The pipeline first creates the spark session, either using drivers required for AWS, if specified in the configuration as discussed above. 

The song data is then read in from the nested directories, and the schema for which to read the json into is defined as follows;

* num_songs - Integer
* artist_id - String
* artist_latitude - Float
* artist_longitude - Float 
* artist_location - String
* artist_name - String
* song_id - String
* title - String
* duration - Float 
* year - Integer

The dimension tables are then created as follows;

*Users* - Reflects the users in the application;

* user_id
* first_name
* last_name
* gender 
* level

*Songs* - Songs in the music database;

* song_id
* title
* artist_id
* year
* duration

*Artists* - Artists in the music database;

* artist_name
* name
* location
* latitude
* longitude

*Time* - The time for which songs have been played;

* start_time
* hour
* day
* week 
* month
* year
* weekday

## Output

Each of these tables is then written to parquet files. The output path is given as a base directory, and the files then written to sub-directories of this.

#### Songs

Songs are written to; `<output-path>/songs`, and are partitioned by year, and artist_id

#### Artists 

Artists are written to `<output-path>/artists`

#### Users

Users are written to `<output-path>/users`

#### Time

Time table is written to `<output-path>/time` and is partitioned by year, and month

#### Songplays

The songplays table is created by joining the songs, and the logs table. Then, the month and year for each start time is then calculated, such that the parquet files can then be partitioned by the month and year.

Hence the time table is written to `<output-path>/songplays`, and partitioned by year and month.