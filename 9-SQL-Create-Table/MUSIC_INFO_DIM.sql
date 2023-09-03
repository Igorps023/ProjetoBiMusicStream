CREATE EXTERNAL TABLE IF NOT EXISTS `music_stream`.`music_info_dim` (
  `track_id` string COMMENT 'unique_value',
  `artist` string COMMENT 'artist_name',
  `spotify_preview_url` string COMMENT 'access_to_song_demo',
  `spotify_id` string COMMENT 'unique_value',
  `tags` string COMMENT 'genres_sep_by_comma',
  `genre` string COMMENT 'music_genre',
  `year` int COMMENT 'year_release',
  `duration_ms` int COMMENT 'duration_of_song_in_miliseconds',
  `danceability` double COMMENT 'parameter_of_a_song',
  `energy` double COMMENT 'parameter_of_a_song',
  `key` int COMMENT 'key_song_was_made',
  `loudness` double COMMENT 'higher_means_louder',
  `mode` int COMMENT 'song_parameter',
  `speechiness` double COMMENT 'higher_means_more_vocals',
  `acousticness` double COMMENT 'self_explainable',
  `instrumentalness` double COMMENT 'the_higher_less_vocals',
  `liveness` double COMMENT 'higher_means_live_music',
  `valence` double COMMENT 'song_parameter',
  `tempo` double COMMENT 'bps_higher_means_faster_beats_per_second',
  `time_signature` int COMMENT 'song_parameter'
  
) COMMENT "All data related to music information and parameters"
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://bkt-musicstream-bi/Files/RefinedZone/Music_Info_dim.parquet/'
TBLPROPERTIES ('classification' = 'parquet');