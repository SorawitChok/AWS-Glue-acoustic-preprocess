import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Acoustic
Acoustic_node1699776954097 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": "\t",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://spotify-user-profile-input/acoustic_features_lfm_id.tsv"],
        "recurse": True,
    },
    transformation_ctx="Acoustic_node1699776954097",
)

# Script generated for node Events
Events_node1699777028034 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": "\t",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://spotify-user-profile-input/events.tsv"],
        "recurse": True,
    },
    transformation_ctx="Events_node1699777028034",
)

# Script generated for node Change Schema
ChangeSchema_node1699776997778 = ApplyMapping.apply(
    frame=Acoustic_node1699776954097,
    mappings=[
        ("track_id", "string", "track_id_1", "string"),
        ("danceability", "string", "danceability", "string"),
        ("energy", "string", "energy", "string"),
        ("key", "string", "key", "string"),
        ("loudness", "string", "loudness", "string"),
        ("mode", "string", "mode", "string"),
        ("speechiness", "string", "speechiness", "string"),
        ("acousticness", "string", "acousticness", "string"),
        ("instrumentalness", "string", "instrumentalness", "string"),
        ("liveness", "string", "liveness", "string"),
        ("valence", "string", "valence", "string"),
        ("tempo", "string", "tempo", "string"),
    ],
    transformation_ctx="ChangeSchema_node1699776997778",
)

# Script generated for node Join
Join_node1699777066930 = Join.apply(
    frame1=ChangeSchema_node1699776997778,
    frame2=Events_node1699777028034,
    keys1=["track_id_1"],
    keys2=["track_id"],
    transformation_ctx="Join_node1699777066930",
)

# Script generated for node Change Schema
ChangeSchema_node1699777112057 = ApplyMapping.apply(
    frame=Join_node1699777066930,
    mappings=[
        ("danceability", "string", "danceability", "string"),
        ("energy", "string", "energy", "string"),
        ("key", "string", "key", "string"),
        ("loudness", "string", "loudness", "string"),
        ("mode", "string", "mode", "string"),
        ("speechiness", "string", "speechiness", "string"),
        ("acousticness", "string", "acousticness", "string"),
        ("instrumentalness", "string", "instrumentalness", "string"),
        ("liveness", "string", "liveness", "string"),
        ("valence", "string", "valence", "string"),
        ("tempo", "string", "tempo", "string"),
        ("user_id", "string", "user_id", "string"),
        ("artist_id", "string", "artist_id", "string"),
        ("album_id", "string", "album_id", "string"),
        ("track_id", "string", "track_id", "string"),
        ("timestamp", "string", "timestamp", "string"),
    ],
    transformation_ctx="ChangeSchema_node1699777112057",
)

# Script generated for node Amazon S3
AmazonS3_node1699777132666 = glueContext.write_dynamic_frame.from_options(
    frame=ChangeSchema_node1699777112057,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://spotify-user-profile-intermediate/data/event-acoustic-preprocessed/",
        "partitionKeys": [],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="AmazonS3_node1699777132666",
)

job.commit()
