# BigData Project For Spotify Database

## Dataset Description
The Popular_Spotify dataset contains information about various music tracks, including track name, artist(s) name, artist count, release year, release month, release day, and various metrics related to the track's performance on different music platforms such as Spotify, Apple Music, Deezer, and Shazam. Additionally, it includes musical attributes like BPM, key, mode, danceability, valence, energy, acousticness, instrumentalness, liveness, and speechiness. The dataset provides insights into the popularity and characteristics of different music tracks across multiple platforms.

## Architecture Choice
We choose Lambda architecture because we want to process data in real-time and in batches separately.

## Architecure
![image](https://github.com/EmnaGaidi/projet_big_data/assets/94928444/e364d1a8-f802-4d03-8c2f-aeb22e31e6b1)

### 1-Data Ingestion
For batch processing : Directly
For Streaming : Kafka
### 2-Data Processing
Streaming : Spark Streaming
Batch : Hadoop MapReduce
### 3-Data Storage
MongoDB
### 4- Data Visualization
Dashboarding : MongoDB Charts

## Dashboard
![Emna's Dashboard (1)](https://github.com/EmnaGaidi/projet_big_data/assets/94928444/41f8c2bf-8bd2-4ed1-af07-5f6112ee5f4e)
