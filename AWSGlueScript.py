import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, lit, array, udf, avg
from pyspark.ml.feature import Imputer
from pyspark.ml.feature import MinMaxScaler, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql.types import ArrayType, DoubleType
from pyspark.ml.linalg import DenseVector

# Script generated for node Custom Transform
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    convert_to_array = udf(lambda vector: vector.toArray().tolist(), ArrayType(DoubleType()))
    acousticDF = dfc.select(list(dfc.keys())[0]).toDF()
    columns = ["danceability", "energy", "key", "loudness", "mode", "speechiness", "acousticness", "instrumentalness", "liveness", "valence", "tempo"]
    
    for col_name in columns:
        acousticDF = acousticDF.withColumn(col_name, col(col_name).cast("float"))
        
    imputer = Imputer(
    inputCols= ["danceability", "energy", "key", "loudness", "mode", "speechiness", "acousticness", "instrumentalness", "liveness", "valence", "tempo"], 
    outputCols= ["danceability", "energy", "key", "loudness", "mode", "speechiness", "acousticness", "instrumentalness", "liveness", "valence", "tempo"], 
    strategy="mean"                 
    )

    model = imputer.fit(acousticDF)
    acousticDF_imputed = model.transform(acousticDF)
    acousticDF_drop = acousticDF_imputed.drop("key","loudness","mode")
    
    assembler = VectorAssembler(inputCols=["danceability", "energy","speechiness", "acousticness", "instrumentalness", "liveness", "valence", "tempo"], outputCol="features")
    scaler = MinMaxScaler(inputCol="features", outputCol="features_scaled")
    pipeline = Pipeline(stages=[assembler, scaler])
    scalerModel = pipeline.fit(acousticDF_drop)
    acousticDF_final = scalerModel.transform(acousticDF_drop)
    acousticDF_final = acousticDF_final.drop("danceability", "energy","speechiness", "acousticness", "instrumentalness", "liveness", "valence", "tempo","features")
    acousticDF_final = acousticDF_final.withColumn("features_scaled", convert_to_array(col("features_scaled")))
    dyf0 = DynamicFrame.fromDF(acousticDF_final, glueContext, "acousticDF_final")
    return DynamicFrameCollection({"acoustic_features_vect": dyf0}, glueContext)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1699624940105 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": "\t"}, connection_type="s3", format="csv", connection_options={"paths": ["s3://spotify-glue-input/acoustic_features_lfm_id.tsv"], "recurse": True}, transformation_ctx="AmazonS3_node1699624940105")

# Script generated for node Custom Transform
CustomTransform_node1699624985083 = MyTransform(glueContext, DynamicFrameCollection({"AmazonS3_node1699624940105": AmazonS3_node1699624940105}, glueContext))

# Script generated for node Amazon S3
AmazonS3_node1699624988455 = glueContext.write_dynamic_frame.from_options(
    frame=CustomTransform_node1699624985083,
    connection_type="s3",
    format="parquet", 
    connection_options={"path": "s3://spotify-glue-output/acoustic_features_vect", "partitionKeys": []},
    format_options={"compression": "snappy"},  
    transformation_ctx="AmazonS3_node1699624988455"
)

job.commit()
