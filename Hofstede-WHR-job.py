import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame


# Script generated for node Imputer
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.ml.feature import Imputer
    from pyspark.sql.functions import col
    from pyspark.ml.feature import MinMaxScaler, VectorAssembler
    from pyspark.ml import Pipeline
    from pyspark.sql.types import ArrayType, DoubleType
    from pyspark.sql.functions import col, lit, array, udf, avg

    country_df = dfc.select(list(dfc.keys())[0]).toDF()
    convert_to_array = udf(
        lambda vector: vector.toArray().tolist(), ArrayType(DoubleType())
    )

    columns = [
        "power_distance",
        "individualism",
        "masculinity",
        "uncertainty_avoidance",
        "long_term_orientation",
        "indulgence",
        "life_ladder",
        "log_gdp_per_capita",
        "social_support",
        "healthy_life_expectancy_at_birth",
        "freedom_to_make_life_choices",
        "generosity",
        "perceptions_of_corruption",
        "positive_affect",
        "negative_affect",
        "confidence_in_national_government",
        "democratic_quality",
        "delivery_quality",
        "standard_deviation_of_ladder_by_country-year",
        "standard_deviation/mean_of_ladder_by_country-year",
        "gini_index_world_bank_estimate_average_2000-15",
        "gini_of_household_income_reported_in_gallup_by_wp5-year",
    ]
    # Cast some columns to float
    for col_name in columns:
        country_df = country_df.withColumn(col_name, col(col_name).cast("float"))

    # Initialize the Imputer
    imputer = Imputer(
        inputCols=[
            "power_distance",
            "individualism",
            "masculinity",
            "uncertainty_avoidance",
            "long_term_orientation",
            "indulgence",
            "life_ladder",
            "log_gdp_per_capita",
            "social_support",
            "healthy_life_expectancy_at_birth",
            "freedom_to_make_life_choices",
            "generosity",
            "perceptions_of_corruption",
            "positive_affect",
            "negative_affect",
            "confidence_in_national_government",
            "democratic_quality",
            "delivery_quality",
            "standard_deviation_of_ladder_by_country-year",
            "standard_deviation/mean_of_ladder_by_country-year",
            "gini_index_world_bank_estimate_average_2000-15",
            "gini_of_household_income_reported_in_gallup_by_wp5-year",
        ],
        outputCols=[
            "power_distance",
            "individualism",
            "masculinity",
            "uncertainty_avoidance",
            "long_term_orientation",
            "indulgence",
            "life_ladder",
            "log_gdp_per_capita",
            "social_support",
            "healthy_life_expectancy_at_birth",
            "freedom_to_make_life_choices",
            "generosity",
            "perceptions_of_corruption",
            "positive_affect",
            "negative_affect",
            "confidence_in_national_government",
            "democratic_quality",
            "delivery_quality",
            "standard_deviation_of_ladder_by_country-year",
            "standard_deviation/mean_of_ladder_by_country-year",
            "gini_index_world_bank_estimate_average_2000-15",
            "gini_of_household_income_reported_in_gallup_by_wp5-year",
        ],
        strategy="mean",
    )

    model = imputer.fit(country_df)

    country_imputed = model.transform(country_df)

    columns_to_scale = [
        "power_distance",
        "individualism",
        "masculinity",
        "uncertainty_avoidance",
        "long_term_orientation",
        "indulgence",
        "life_ladder",
        "log_gdp_per_capita",
        "social_support",
        "healthy_life_expectancy_at_birth",
        "freedom_to_make_life_choices",
        "generosity",
        "perceptions_of_corruption",
        "positive_affect",
        "negative_affect",
        "confidence_in_national_government",
        "democratic_quality",
        "delivery_quality",
        "standard_deviation_of_ladder_by_country-year",
        "standard_deviation/mean_of_ladder_by_country-year",
        "gini_index_world_bank_estimate_average_2000-15",
        "gini_of_household_income_reported_in_gallup_by_wp5-year",
    ]
    assemblers = [
        VectorAssembler(inputCols=columns_to_scale, outputCol="country_features")
    ]
    scalers = [
        MinMaxScaler(inputCol="country_features", outputCol="country_features_scaled")
    ]
    pipeline = Pipeline(stages=assemblers + scalers)
    scalerModel = pipeline.fit(country_imputed)
    scaledData = scalerModel.transform(country_imputed)
    scaledData_fin = scaledData.drop(
        "year",
        "gini_index_world_bank_estimate",
        "country_features",
        "power_distance",
        "individualism",
        "masculinity",
        "uncertainty_avoidance",
        "long_term_orientation",
        "indulgence",
        "life_ladder",
        "log_gdp_per_capita",
        "social_support",
        "healthy_life_expectancy_at_birth",
        "freedom_to_make_life_choices",
        "generosity",
        "perceptions_of_corruption",
        "positive_affect",
        "negative_affect",
        "confidence_in_national_government",
        "democratic_quality",
        "delivery_quality",
        "standard_deviation_of_ladder_by_country-year",
        "standard_deviation/mean_of_ladder_by_country-year",
        "gini_index_world_bank_estimate_average_2000-15",
        "gini_of_household_income_reported_in_gallup_by_wp5-year",
    )

    scaledData_fin = scaledData_fin.withColumn(
        "country_features_scaled", convert_to_array(col("country_features_scaled"))
    )

    dyf0 = DynamicFrame.fromDF(scaledData_fin, glueContext, "scaledData_fin")
    return DynamicFrameCollection({"scaledData_fin": dyf0}, glueContext)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Hofstede Preprocessed
HofstedePreprocessed_node1699777849361 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://spotify-user-profile-intermediate/data/hofstede-preprocessed/"],
        "recurse": True,
    },
    transformation_ctx="HofstedePreprocessed_node1699777849361",
)

# Script generated for node WHR Preprocessed
WHRPreprocessed_node1699777896430 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://spotify-user-profile-intermediate/data/WHR-preprocessed/"],
        "recurse": True,
    },
    transformation_ctx="WHRPreprocessed_node1699777896430",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1699777993900 = ApplyMapping.apply(
    frame=WHRPreprocessed_node1699777896430,
    mappings=[
        ("country", "string", "country_1", "string"),
        ("year", "string", "year", "string"),
        ("life_ladder", "string", "life_ladder", "string"),
        ("log_gdp_per_capita", "string", "log_gdp_per_capita", "string"),
        ("social_support", "string", "social_support", "string"),
        (
            "healthy_life_expectancy_at_birth",
            "string",
            "healthy_life_expectancy_at_birth",
            "string",
        ),
        (
            "freedom_to_make_life_choices",
            "string",
            "freedom_to_make_life_choices",
            "string",
        ),
        ("generosity", "string", "generosity", "string"),
        ("perceptions_of_corruption", "string", "perceptions_of_corruption", "string"),
        ("positive_affect", "string", "positive_affect", "string"),
        ("negative_affect", "string", "negative_affect", "string"),
        (
            "confidence_in_national_government",
            "string",
            "confidence_in_national_government",
            "string",
        ),
        ("democratic_quality", "string", "democratic_quality", "string"),
        ("delivery_quality", "string", "delivery_quality", "string"),
        (
            "standard_deviation_of_ladder_by_country-year",
            "string",
            "standard_deviation_of_ladder_by_country-year",
            "string",
        ),
        (
            "standard_deviation/mean_of_ladder_by_country-year",
            "string",
            "standard_deviation/mean_of_ladder_by_country-year",
            "string",
        ),
        (
            "gini_index_world_bank_estimate",
            "string",
            "gini_index_world_bank_estimate",
            "string",
        ),
        (
            "gini_index_world_bank_estimate_average_2000-15",
            "string",
            "gini_index_world_bank_estimate_average_2000-15",
            "string",
        ),
        (
            "gini_of_household_income_reported_in_gallup_by_wp5-year",
            "string",
            "gini_of_household_income_reported_in_gallup_by_wp5-year",
            "string",
        ),
    ],
    transformation_ctx="RenamedkeysforJoin_node1699777993900",
)

# Script generated for node Join
Join_node1699777949305 = Join.apply(
    frame1=HofstedePreprocessed_node1699777849361,
    frame2=RenamedkeysforJoin_node1699777993900,
    keys1=["country"],
    keys2=["country_1"],
    transformation_ctx="Join_node1699777949305",
)

# Script generated for node Change Schema
ChangeSchema_node1699778064769 = ApplyMapping.apply(
    frame=Join_node1699777949305,
    mappings=[
        ("no", "string", "no", "string"),
        ("ctr", "string", "ctr", "string"),
        ("country", "string", "country", "string"),
        ("power_distance", "string", "power_distance", "string"),
        ("individualism", "string", "individualism", "string"),
        ("masculinity", "string", "masculinity", "string"),
        ("uncertainty_avoidance", "string", "uncertainty_avoidance", "string"),
        ("long_term_orientation", "string", "long_term_orientation", "string"),
        ("indulgence", "string", "indulgence", "string"),
        ("year", "string", "year", "string"),
        ("life_ladder", "string", "life_ladder", "string"),
        ("log_gdp_per_capita", "string", "log_gdp_per_capita", "string"),
        ("social_support", "string", "social_support", "string"),
        (
            "healthy_life_expectancy_at_birth",
            "string",
            "healthy_life_expectancy_at_birth",
            "string",
        ),
        (
            "freedom_to_make_life_choices",
            "string",
            "freedom_to_make_life_choices",
            "string",
        ),
        ("generosity", "string", "generosity", "string"),
        ("perceptions_of_corruption", "string", "perceptions_of_corruption", "string"),
        ("positive_affect", "string", "positive_affect", "string"),
        ("negative_affect", "string", "negative_affect", "string"),
        (
            "confidence_in_national_government",
            "string",
            "confidence_in_national_government",
            "string",
        ),
        ("democratic_quality", "string", "democratic_quality", "string"),
        ("delivery_quality", "string", "delivery_quality", "string"),
        (
            "standard_deviation_of_ladder_by_country-year",
            "string",
            "standard_deviation_of_ladder_by_country-year",
            "string",
        ),
        (
            "standard_deviation/mean_of_ladder_by_country-year",
            "string",
            "standard_deviation/mean_of_ladder_by_country-year",
            "string",
        ),
        (
            "gini_index_world_bank_estimate",
            "string",
            "gini_index_world_bank_estimate",
            "string",
        ),
        (
            "gini_index_world_bank_estimate_average_2000-15",
            "string",
            "gini_index_world_bank_estimate_average_2000-15",
            "string",
        ),
        (
            "gini_of_household_income_reported_in_gallup_by_wp5-year",
            "string",
            "gini_of_household_income_reported_in_gallup_by_wp5-year",
            "string",
        ),
    ],
    transformation_ctx="ChangeSchema_node1699778064769",
)

# Script generated for node Imputer
Imputer_node1699778102729 = MyTransform(
    glueContext,
    DynamicFrameCollection(
        {"ChangeSchema_node1699778064769": ChangeSchema_node1699778064769}, glueContext
    ),
)

# Script generated for node Select From Collection
SelectFromCollection_node1699778133513 = SelectFromCollection.apply(
    dfc=Imputer_node1699778102729,
    key=list(Imputer_node1699778102729.keys())[0],
    transformation_ctx="SelectFromCollection_node1699778133513",
)

# Script generated for node Amazon S3
AmazonS3_node1699778149873 = glueContext.write_dynamic_frame.from_options(
    frame=SelectFromCollection_node1699778133513,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://spotify-user-profile-intermediate/data/Hofstede-WHR/",
        "partitionKeys": [],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="AmazonS3_node1699778149873",
)

job.commit()
