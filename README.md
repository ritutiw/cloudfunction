# cloudfunction
Gcp cloud functions

 This function load all dim csv files into bigquery before that process those files and send in processed_files and then load into bigquery.
 Issue with this code is it works fine with one table but as soon as i change logic to consider all files memory gets a concern.
 
Could you please advise on it how to load all files into Bq after doing necessary transformation keeping memory in mind.
