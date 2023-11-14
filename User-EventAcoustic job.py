import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame


# Script generated for node Custom Transform
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.ml.feature import Imputer
    from pyspark.ml.feature import MinMaxScaler, VectorAssembler
    from pyspark.ml import Pipeline
    from pyspark.sql.types import ArrayType, DoubleType
    from pyspark.sql.functions import avg, max, stddev, col, udf

    glueContext._jsc.hadoopConfiguration().set(
        "mapred.output.committer.class",
        "org.apache.hadoop.mapred.DirectFileOutputCommitter",
    )

    convert_to_array = udf(
        lambda vector: vector.toArray().tolist(), ArrayType(DoubleType())
    )
    event_acous_user_df = dfc.select(list(dfc.keys())[0]).toDF()

    user_avg_acous_df = event_acous_user_df.groupBy("user_id", "country").agg(
        avg("danceability").alias("avg_danceability"),
        avg("energy").alias("avg_energy"),
        avg("loudness").alias("avg_loudness"),
        avg("speechiness").alias("avg_speechiness"),
        avg("acousticness").alias("avg_acousticness"),
        avg("instrumentalness").alias("avg_instrumentalness"),
        avg("liveness").alias("avg_liveness"),
        avg("valence").alias("avg_valence"),
        avg("tempo").alias("avg_tempo"),
        stddev("danceability").alias("std_danceability"),
        stddev("energy").alias("std_energy"),
        stddev("loudness").alias("std_loudness"),
        stddev("speechiness").alias("std_speechiness"),
        stddev("acousticness").alias("std_acousticness"),
        stddev("instrumentalness").alias("std_instrumentalness"),
        stddev("liveness").alias("std_liveness"),
        stddev("valence").alias("std_valence"),
        stddev("tempo").alias("std_tempo"),
        max("age").alias("age"),
    )

    for column in [
        "avg_danceability",
        "avg_energy",
        "avg_loudness",
        "avg_speechiness",
        "avg_acousticness",
        "avg_instrumentalness",
        "avg_liveness",
        "avg_valence",
        "avg_tempo",
        "std_danceability",
        "std_energy",
        "std_loudness",
        "std_speechiness",
        "std_acousticness",
        "std_instrumentalness",
        "std_liveness",
        "std_valence",
        "std_tempo",
        "age",
    ]:
        user_avg_acous_df = user_avg_acous_df.withColumn(
            column, col(column).cast("float")
        ).fillna(0, subset=[column])

    # Initialize the Imputer
    imputer = Imputer(
        inputCols=[
            "std_danceability",
            "std_energy",
            "std_loudness",
            "std_speechiness",
            "std_acousticness",
            "std_instrumentalness",
            "std_liveness",
            "std_valence",
            "std_tempo",
        ],
        outputCols=[
            "std_danceability",
            "std_energy",
            "std_loudness",
            "std_speechiness",
            "std_acousticness",
            "std_instrumentalness",
            "std_liveness",
            "std_valence",
            "std_tempo",
        ],
        strategy="mean",
    )

    model = imputer.fit(user_avg_acous_df)

    columns_to_scale = [
        "avg_danceability",
        "avg_energy",
        "avg_loudness",
        "avg_speechiness",
        "avg_acousticness",
        "avg_instrumentalness",
        "avg_liveness",
        "avg_valence",
        "avg_tempo",
        "std_danceability",
        "std_energy",
        "std_loudness",
        "std_speechiness",
        "std_acousticness",
        "std_instrumentalness",
        "std_liveness",
        "std_valence",
        "std_tempo",
        "age",
    ]
    assemblers = [
        VectorAssembler(inputCols=columns_to_scale, outputCol="user_features")
    ]
    scalers = [MinMaxScaler(inputCol="user_features", outputCol="user_features_scaled")]
    pipeline = Pipeline(stages=assemblers + scalers)
    scalerModel = pipeline.fit(user_avg_acous_df)
    scalerModel.write().overwrite().save(
        "s3://spotify-user-profile-intermediate/AcousAgeScaler"
    )
    user_avg_acous_scaled = scalerModel.transform(user_avg_acous_df)

    user_avg_acous_df = model.transform(user_avg_acous_df)
    user_avg_acous_scaled = user_avg_acous_scaled.drop(
        "user_features",
        "avg_danceability",
        "avg_energy",
        "avg_loudness",
        "avg_speechiness",
        "avg_acousticness",
        "avg_instrumentalness",
        "avg_liveness",
        "avg_valence",
        "avg_tempo",
        "std_danceability",
        "std_energy",
        "std_loudness",
        "std_speechiness",
        "std_acousticness",
        "std_instrumentalness",
        "std_liveness",
        "std_valence",
        "std_tempo",
        "age",
    )
    user_avg_acous_scaled = user_avg_acous_scaled.withColumn(
        "user_features_scaled", convert_to_array(col("user_features_scaled"))
    )

    dyf0 = DynamicFrame.fromDF(
        user_avg_acous_scaled, glueContext, "user_avg_acous_scaled"
    )
    return DynamicFrameCollection({"user_avg_acous_scaled": dyf0}, glueContext)


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node EventAcoustic Preprocess
EventAcousticPreprocess_node1699778476932 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": [
            "s3://spotify-user-profile-intermediate/data/event-acoustic-preprocessed/"
        ],
        "recurse": True,
    },
    transformation_ctx="EventAcousticPreprocess_node1699778476932",
)

# Script generated for node Users Preprocessed
UsersPreprocessed_node1699778437736 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://spotify-user-profile-intermediate/data/Users-preprocessed/"],
        "recurse": True,
    },
    transformation_ctx="UsersPreprocessed_node1699778437736",
)

# Script generated for node SQL Query
SqlQuery608 = """
SELECT EventAcousTB.*, UsersTB.*
FROM EventAcousTB
INNER JOIN UsersTB ON EventAcousTB.user_id=UsersTB.user_id;
"""
SQLQuery_node1699959393640 = sparkSqlQuery(
    glueContext,
    query=SqlQuery608,
    mapping={
        "EventAcousTB": EventAcousticPreprocess_node1699778476932,
        "UsersTB": UsersPreprocessed_node1699778437736,
    },
    transformation_ctx="SQLQuery_node1699959393640",
)

# Script generated for node Custom Transform
CustomTransform_node1699955026484 = MyTransform(
    glueContext,
    DynamicFrameCollection(
        {"SQLQuery_node1699959393640": SQLQuery_node1699959393640}, glueContext
    ),
)

# Script generated for node Select From Collection 1
SelectFromCollection1_node1699955276046 = SelectFromCollection.apply(
    dfc=CustomTransform_node1699955026484,
    key=list(CustomTransform_node1699955026484.keys())[0],
    transformation_ctx="SelectFromCollection1_node1699955276046",
)

# Script generated for node Amazon S3
AmazonS3_node1699779732384 = glueContext.write_dynamic_frame.from_options(
    frame=SelectFromCollection1_node1699955276046,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://spotify-user-profile-intermediate/data/Users-EventAcoustic/",
        "partitionKeys": [],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="AmazonS3_node1699779732384",
)

job.commit()
