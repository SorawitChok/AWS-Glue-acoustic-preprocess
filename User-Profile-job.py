import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame


# Script generated for node country avg age
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import desc
    from pyspark.sql.types import FloatType
    from pyspark.sql.functions import col
    from pyspark.sql.functions import when

    userDF = dfc.select(list(dfc.keys())[0]).toDF()

    userCountry = userDF.groupBy("country").count().orderBy(desc("count"))
    userCountry.createOrReplaceTempView("user_country_tb")
    userCountry_new = spark.sql("select * from user_country_tb where count >= 200")

    userCountry_new.createOrReplaceTempView("user_country_new_tb")
    country_gt_200 = spark.sql("select country from user_country_new_tb")
    country_gt_200_list = country_gt_200.rdd.flatMap(lambda x: x).collect()

    userDF_new = userDF.filter(userDF.country.isin(country_gt_200_list)).filter(
        userDF.country != "ID"
    )

    userDF_drop_neg_age = userDF_new.where("age >= 0")
    userDF_drop_neg_age = userDF_drop_neg_age.withColumn(
        "age", col("age").cast(FloatType())
    )
    userDF_mean = userDF_drop_neg_age.groupBy("country").mean("age")

    filled_user_df = userDF_new.join(userDF_mean, "country", "left")
    filled_user_df = filled_user_df.withColumn(
        "age", when(col("age") == -1, col("avg(age)")).otherwise(col("age"))
    )
    filled_user_df = filled_user_df.drop("avg(age)")
    userDF_final = filled_user_df.fillna("n", subset=["gender"])

    dyf0 = DynamicFrame.fromDF(userDF_final, glueContext, "userDF_final")
    return DynamicFrameCollection({"userDF_final": dyf0}, glueContext)


# Script generated for node Concatenate features
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import udf
    from pyspark.ml.linalg import Vectors, VectorUDT

    user_profileDF = dfc.select(list(dfc.keys())[0]).toDF()

    concat = udf(lambda v, e: Vectors.dense(list(v) + list(e)), VectorUDT())
    user_profileDF = user_profileDF.select(
        "user_id",
        "ctr",
        concat(
            user_profileDF.user_features_scaled, user_profileDF.country_features_scaled
        ).alias("user_profile"),
    )

    dyf0 = DynamicFrame.fromDF(user_profileDF, glueContext, "user_profileDF")
    return DynamicFrameCollection({"user_profileDF": dyf0}, glueContext)


# Script generated for node Impute scale HOF_WHR
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.ml.feature import Imputer
    from pyspark.sql.functions import col
    from pyspark.ml.feature import MinMaxScaler, VectorAssembler
    from pyspark.ml import Pipeline

    country_df = dfc.select(list(dfc.keys())[0]).toDF()

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

    dyf0 = DynamicFrame.fromDF(scaledData_fin, glueContext, "scaledData_fin")
    return DynamicFrameCollection({"scaledData_fin": dyf0}, glueContext)


# Script generated for node impute scale user_acosutic
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.ml.feature import Imputer
    from pyspark.ml.feature import MinMaxScaler, VectorAssembler
    from pyspark.ml import Pipeline

    user_avg_acous_df = dfc.select(list(dfc.keys())[0]).toDF()

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

    dyf0 = DynamicFrame.fromDF(
        user_avg_acous_scaled, glueContext, "user_avg_acous_scaled"
    )
    return DynamicFrameCollection({"user_avg_acous_scaled": dyf0}, glueContext)


# Script generated for node Custom Transform WHR rename col
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    worldHappinessDF = dfc.select(list(dfc.keys())[0]).toDF()

    ori_columns_name = worldHappinessDF.columns
    new_columns_name = list()
    for col in ori_columns_name:
        name = col.lower()
        new_col = name.replace(" ", "_")
        new_col = new_col.replace("(", "")
        new_col = new_col.replace(")", "")
        new_col = new_col.replace(",", "")
        new_columns_name.append(new_col)

    worldHappinessDF = worldHappinessDF.toDF(*new_columns_name)
    worldHappinessDF = worldHappinessDF.filter(worldHappinessDF.year == 2016)
    dyf0 = DynamicFrame.fromDF(worldHappinessDF, glueContext, "worldHappinessDF")
    return DynamicFrameCollection({"worldHappinessDF": dyf0}, glueContext)


# Script generated for node Custom Transform hof country name
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import when

    hofstedeDF = dfc.select(list(dfc.keys())[0]).toDF()

    hofstedeDF = hofstedeDF.withColumn(
        "country",
        when(hofstedeDF.country == "Czech Rep", "Czech Republic")
        .when(hofstedeDF.country == "U.S.A.", "United States")
        .when(hofstedeDF.country == "Great Britain", "United Kingdom")
        .otherwise(hofstedeDF.country),
    )
    dyf0 = DynamicFrame.fromDF(hofstedeDF, glueContext, "hofstedeDF")
    return DynamicFrameCollection({"hofstedeDF": dyf0}, glueContext)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Events
Events_node1699688592460 = glueContext.create_dynamic_frame.from_options(
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
    transformation_ctx="Events_node1699688592460",
)

# Script generated for node Users
Users_node1699688549401 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": "\t",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://spotify-user-profile-input/users.tsv"],
        "recurse": True,
    },
    transformation_ctx="Users_node1699688549401",
)

# Script generated for node WHR
WHR_node1699696773442 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": "\t",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://spotify-user-profile-input/world_happiness_report_2018.tsv"],
        "recurse": True,
    },
    transformation_ctx="WHR_node1699696773442",
)

# Script generated for node Acoustic
Acoustic_node1699688550456 = glueContext.create_dynamic_frame.from_options(
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
    transformation_ctx="Acoustic_node1699688550456",
)

# Script generated for node Hofstede
Hofstede_node1699696711654 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": "\t",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://spotify-user-profile-input/hofstede.tsv"],
        "recurse": True,
    },
    transformation_ctx="Hofstede_node1699696711654",
)

# Script generated for node country avg age
countryavgage_node1699695794095 = MyTransform(
    glueContext,
    DynamicFrameCollection(
        {"Users_node1699688549401": Users_node1699688549401}, glueContext
    ),
)

# Script generated for node Custom Transform WHR rename col
CustomTransformWHRrenamecol_node1699696991549 = MyTransform(
    glueContext,
    DynamicFrameCollection(
        {"WHR_node1699696773442": WHR_node1699696773442}, glueContext
    ),
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1699689111395 = ApplyMapping.apply(
    frame=Acoustic_node1699688550456,
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
    transformation_ctx="RenamedkeysforJoin_node1699689111395",
)

# Script generated for node Custom Transform hof country name
CustomTransformhofcountryname_node1699696858571 = MyTransform(
    glueContext,
    DynamicFrameCollection(
        {"Hofstede_node1699696711654": Hofstede_node1699696711654}, glueContext
    ),
)

# Script generated for node Select From Collection user fin df
SelectFromCollectionuserfindf_node1699696570735 = SelectFromCollection.apply(
    dfc=countryavgage_node1699695794095,
    key=list(countryavgage_node1699695794095.keys())[0],
    transformation_ctx="SelectFromCollectionuserfindf_node1699696570735",
)

# Script generated for node Select From Collection WHR
SelectFromCollectionWHR_node1699697322586 = SelectFromCollection.apply(
    dfc=CustomTransformWHRrenamecol_node1699696991549,
    key=list(CustomTransformWHRrenamecol_node1699696991549.keys())[0],
    transformation_ctx="SelectFromCollectionWHR_node1699697322586",
)

# Script generated for node Join Event acoustic
JoinEventacoustic_node1699688647347 = Join.apply(
    frame1=Events_node1699688592460,
    frame2=RenamedkeysforJoin_node1699689111395,
    keys1=["track_id"],
    keys2=["track_id_1"],
    transformation_ctx="JoinEventacoustic_node1699688647347",
)

# Script generated for node Select From Collection hof
SelectFromCollectionhof_node1699697307866 = SelectFromCollection.apply(
    dfc=CustomTransformhofcountryname_node1699696858571,
    key=list(CustomTransformhofcountryname_node1699696858571.keys())[0],
    transformation_ctx="SelectFromCollectionhof_node1699697307866",
)

# Script generated for node final user df
finaluserdf_node1699696520077 = ApplyMapping.apply(
    frame=SelectFromCollectionuserfindf_node1699696570735,
    mappings=[
        ("user_id", "string", "user_id_1", "string"),
        ("country", "string", "country", "string"),
        ("age", "string", "age", "string"),
        ("gender", "string", "gender", "string"),
        ("playcount", "string", "playcount", "string"),
        ("registered_unixtime", "string", "registered_unixtime", "string"),
    ],
    transformation_ctx="finaluserdf_node1699696520077",
)

# Script generated for node Change Schema WHR country
ChangeSchemaWHRcountry_node1699697338633 = ApplyMapping.apply(
    frame=SelectFromCollectionWHR_node1699697322586,
    mappings=[
        ("country", "string", "country_1", "string"),
        ("Life Ladder", "string", "Life Ladder", "string"),
        ("Log GDP per capita", "string", "Log GDP per capita", "string"),
        ("Social support", "string", "Social support", "string"),
        (
            "Healthy life expectancy at birth",
            "string",
            "Healthy life expectancy at birth",
            "string",
        ),
        (
            "Freedom to make life choices",
            "string",
            "Freedom to make life choices",
            "string",
        ),
        ("Generosity", "string", "Generosity", "string"),
        ("Perceptions of corruption", "string", "Perceptions of corruption", "string"),
        ("Positive affect", "string", "Positive affect", "string"),
        ("Negative affect", "string", "Negative affect", "string"),
        (
            "Confidence in national government",
            "string",
            "Confidence in national government",
            "string",
        ),
        ("Democratic Quality", "string", "Democratic Quality", "string"),
        ("Delivery Quality", "string", "Delivery Quality", "string"),
        (
            "Standard deviation of ladder by country-year",
            "string",
            "Standard deviation of ladder by country-year",
            "string",
        ),
        (
            "Standard deviation/Mean of ladder by country-year",
            "string",
            "Standard deviation/Mean of ladder by country-year",
            "string",
        ),
        (
            "`GINI index (World Bank estimate), average 2000-15`",
            "string",
            "`GINI index (World Bank estimate), average 2000-15`",
            "string",
        ),
        (
            "gini of household income reported in Gallup, by wp5-year",
            "string",
            "gini of household income reported in Gallup, by wp5-year",
            "string",
        ),
    ],
    transformation_ctx="ChangeSchemaWHRcountry_node1699697338633",
)

# Script generated for node Change Schema event acous
ChangeSchemaeventacous_node1699689245528 = ApplyMapping.apply(
    frame=JoinEventacoustic_node1699688647347,
    mappings=[
        ("user_id", "string", "user_id", "string"),
        ("artist_id", "string", "artist_id", "string"),
        ("album_id", "string", "album_id", "string"),
        ("track_id", "string", "track_id", "string"),
        ("timestamp", "string", "timestamp", "string"),
        ("danceability", "string", "danceability", "string"),
        ("energy", "string", "energy", "string"),
        ("loudness", "string", "loudness", "string"),
        ("speechiness", "string", "speechiness", "string"),
        ("acousticness", "string", "acousticness", "string"),
        ("instrumentalness", "string", "instrumentalness", "string"),
        ("liveness", "string", "liveness", "string"),
        ("valence", "string", "valence", "string"),
        ("tempo", "string", "tempo", "string"),
    ],
    transformation_ctx="ChangeSchemaeventacous_node1699689245528",
)

# Script generated for node Join
Join_node1699697467473 = Join.apply(
    frame1=SelectFromCollectionhof_node1699697307866,
    frame2=ChangeSchemaWHRcountry_node1699697338633,
    keys1=["country"],
    keys2=["country_1"],
    transformation_ctx="Join_node1699697467473",
)

# Script generated for node Join
Join_node1699696619474 = Join.apply(
    frame1=finaluserdf_node1699696520077,
    frame2=ChangeSchemaeventacous_node1699689245528,
    keys1=["user_id_1"],
    keys2=["user_id"],
    transformation_ctx="Join_node1699696619474",
)

# Script generated for node Impute scale HOF_WHR
ImputescaleHOF_WHR_node1699697723310 = MyTransform(
    glueContext,
    DynamicFrameCollection(
        {"Join_node1699697467473": Join_node1699697467473}, glueContext
    ),
)

# Script generated for node impute scale user_acosutic
imputescaleuser_acosutic_node1699698197557 = MyTransform(
    glueContext,
    DynamicFrameCollection(
        {"Join_node1699696619474": Join_node1699696619474}, glueContext
    ),
)

# Script generated for node HOF_WHR Collection
HOF_WHRCollection_node1699698975280 = SelectFromCollection.apply(
    dfc=ImputescaleHOF_WHR_node1699697723310,
    key=list(ImputescaleHOF_WHR_node1699697723310.keys())[0],
    transformation_ctx="HOF_WHRCollection_node1699698975280",
)

# Script generated for node user_acous collection
user_acouscollection_node1699698941197 = SelectFromCollection.apply(
    dfc=imputescaleuser_acosutic_node1699698197557,
    key=list(imputescaleuser_acosutic_node1699698197557.keys())[0],
    transformation_ctx="user_acouscollection_node1699698941197",
)

# Script generated for node Join
Join_node1699698992371 = Join.apply(
    frame1=user_acouscollection_node1699698941197,
    frame2=HOF_WHRCollection_node1699698975280,
    keys1=["country"],
    keys2=["ctr"],
    transformation_ctx="Join_node1699698992371",
)

# Script generated for node Concatenate features
Concatenatefeatures_node1699699171267 = MyTransform(
    glueContext,
    DynamicFrameCollection(
        {"Join_node1699698992371": Join_node1699698992371}, glueContext
    ),
)

# Script generated for node Final collection
Finalcollection_node1699699461840 = SelectFromCollection.apply(
    dfc=Concatenatefeatures_node1699699171267,
    key=list(Concatenatefeatures_node1699699171267.keys())[0],
    transformation_ctx="Finalcollection_node1699699461840",
)

# Script generated for node Amazon S3
AmazonS3_node1699699424419 = glueContext.write_dynamic_frame.from_options(
    frame=Finalcollection_node1699699461840,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://spotify-user-profile-output/user-profile/",
        "partitionKeys": [],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="AmazonS3_node1699699424419",
)

job.commit()
