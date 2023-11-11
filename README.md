# Spotify Music Recommendation AWS Glue Job

This repository contains an AWS Glue job developed in Python using PySpark for Spotify music recommendation. The Glue job is designed for using as Extract-Transform-Load (ETL), which responsible for preprocessing the data.

## Prerequisites

Before running this AWS Glue job, ensure you have the following:

- An AWS account with the necessary permissions to run AWS Glue jobs.
- Required AWS Glue job resources and configurations set up in your AWS environment.
- Python libraries or dependencies required for the Glue job. You can specify the dependencies in the job script or provide a requirements file.

## Dataset
The dataset use in this project can be derived from [Culture-Aware Music Recommendation Dataset](https://zenodo.org/records/3477842) provided by Eva Zangerle.

## Usage

Follow these steps to run the AWS Glue job for preprocessing the data for Spotify music recommendation:

1. Clone this repository to your local machine or an Amazon EC2 instance:

   ```bash
   git clone https://github.com/yourusername/spotify-music-recommendation.git
   cd spotify-music-recommendation
