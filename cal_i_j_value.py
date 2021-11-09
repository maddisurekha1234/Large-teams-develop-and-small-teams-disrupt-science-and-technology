from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import udf
from pyspark.sql.functions import broadcast

from datetime import datetime
import pyspark.sql.functions as F
from collections import defaultdict


def removeConstUDF(element):
    # print("enter")
    val = element.split(" ")
    # print(val[0])
    # print(val[1])
    # print(val[2])
    final_date_string = val[0] + "_" + val[1]
    # print(final_date_string)
    return final_date_string


def init_spark():
    print("creating spark session...gcp1-12G-e1x-run for I value")
    # spark = SparkSession.builder.config("spark.driver.memory", "6g").config("spark.driver.bindAddress","127.0.0.1").master("local[*]").config("spark.hadoop.validateOutputSpecs", "false").appName("Project1").getOrCreate()

    spark = SparkSession.builder.config("spark.hadoop.validateOutputSpecs", "false").appName(
        "Project-foc").getOrCreate()

    # spark = SparkSession.builder.config("spark.driver.memory", "12g").config('spark.executor.memory', '10g').config(
    #     'spark.executor.cores', '3').config('spark.num.executors', '4').config("spark.hadoop.validateOutputSpecs",
    #                                                                            "false").appName(
    #     "Project-foc").getOrCreate()
    print("Done...... creating spark session...")
    sc = spark.sparkContext
    return spark, sc


def read_input(spark, file_name, delimit, fmt, header):
    print("Reading files..")

    df = spark.read.format(fmt).option("header", header).option("inferSchema", "True").options(
        delimiter=delimit).load(file_name)

    print("Done Reading files..")
    return df


def filter_df(df, event):
    print("get_push_events")
    df_filter = df.filter(df.Type == event)
    return df_filter


def get_core_contributors(spark, df):
    print("Get core contributors")
    df_only_push_events = filter_df(df, 'PushEvent')
    df_only_push_events.createOrReplaceTempView("df_only_push_events_tbl")

    df_1 = spark.sql(
        "select actor_id,repo_id, count(*) as NO_OF_PUSH_EVENT from df_only_push_events_tbl group by actor_id,repo_id ORDER BY repo_id")
    # df_1.show(5)
    # df_1.printSchema

    df_2 = spark.sql(
        "select repo_id, count(*)/count(distinct actor_id) as AVG_PUSH_EVENT from df_only_push_events_tbl where type='PushEvent' group by repo_id ORDER BY repo_id")

    # df_2.show(5)
    # df_2.printSchema

    df_1.createOrReplaceTempView("df_1_tbl")
    df_2.createOrReplaceTempView("df_2_tbl")

    df_3 = spark.sql(
        'select A.actor_id, A.repo_id from df_1_tbl as A left join df_2_tbl as B on A.repo_id=B.repo_id where A.NO_OF_PUSH_EVENT >= B.AVG_PUSH_EVENT')

    # df_3.show(5)
    # df_3.printSchema

    df_3.createOrReplaceTempView("df_3_tbl")
    df_core_contributor = spark.sql("select distinct repo_id,actor_id from df_3_tbl GROUP BY repo_id, actor_id")
    print("FINAL CORE CONTRIBUTORS LIST...")
    # df_core_contributor.show(5)
    # print("count of final records: ", df_core_contributor.count())
    return df_core_contributor


def read_test(spark, schema, location, header="False"):
    print("read test...")
    df = spark.read.format("csv").option("header", header).schema(schema).options(
        delimiter=",").load(location)
    # df.printSchema()
    # df.show(5)
    # print("count of core contributor read: ", df.count())
    df_cc_raw = df
    # df_cc_raw.withColumn("last_interact_dt", F.when(df_cc_raw.new_column == df_cc_raw.interaction_dtl_1,
    #                                                   df_cc_raw.dt))
    # df_cc_raw.withColumn("total_interactions", F.when(df_cc_raw.new_column == df_cc_raw.interaction_dtl_1, df_cc_raw.total_interactions + F.lit(1)))
    #
    # df_cc_raw.withColumn("intent", F.when(df_cc_raw.new_column != df_cc_raw.interaction_dtl_1, "null" ))
    return df


def write(df, format, location, mode):
    print("started writing")
    # df.repartition(1).write.mode('overwrite').csv("data/project-out-cc1")
    # df.coalesce(1).write.mode(mode).format(format).save(location)
    df.write.mode(mode).format(format).save(location, header='true')
    print("done writing")


# final_df.repartition(1).write.mode('overwrite').csv("data/project-out-cc1")

def get_focal_references(spark, df_cc, df_raw):
    print("get_focal_references-upd")
    df_raw_only_push_events = filter_df(df_raw, 'PushEvent')
    df_raw_only_push_events.createOrReplaceTempView("df_raw_only_push_events_tbl")

    df_raw_only_push_events_min_max_dates = spark.sql(
        "select repo_id,actor_id, min(created_at) as min_d,max(created_at) as max_d from df_raw_only_push_events_tbl group by repo_id,actor_id ORDER BY repo_id")
    # write(df_raw_only_push_events_min_max_dates,"csv","data/project-surr/temp-out/min-max-100","overwrite")
    # write(df_raw_only_push_events_min_max_dates, "csv", "gs://project-work-aug/project-temp-out/df_raw_only_push_events_min_max_dates", "overwrite")

    # join core contributors with min max df
    df_cc = df_cc.withColumnRenamed("cc_repo_id", "focal_repo_id")

    print("df_cc.printSchema()")
    df_cc.printSchema()
    print("df_raw_only_push_events_min_max_dates")
    df_raw_only_push_events_min_max_dates.printSchema()

    # df_cc12 = broadcast(df_cc)
    df_cc.createOrReplaceTempView("df_cc_tbl")
    df_raw_only_push_events_min_max_dates.createOrReplaceTempView("df_raw_only_push_events_min_max_dates_tbl")

    int_df_cc_min_max = spark.sql(

        "select  f.focal_repo_id, e.actor_id, e.min_d, e.max_d from df_raw_only_push_events_min_max_dates_tbl e INNER JOIN df_cc_tbl f ON e.actor_id == f.actor_id AND e.repo_id == f.focal_repo_id")

    print("after  inner join:")
    int_df_cc_min_max.show(20)
    int_df_cc_min_max.printSchema()
    # df_cc_raw1 = df_cc_raw.drop_duplicates()
    # print("after GCP inner join drop: ", df_cc_raw1.count())
    # df_raw_only_push_events_min_max_dates1 = df_raw_only_push_events_min_max_dates.drop_duplicates()
    # write(df_cc_raw, "csv", "gs://project-work-aug/project-temp-out/df-cc-raw", "overwrite")
    # write(df_cc_raw, "csv", "data/project-surr/temp-out/df-cc-raw-100", "overwrite")

    # df_cc_raw.createOrReplaceTempView("df_cc_raw_tbl")
    # exit(0)
    print("df_only_pull_events.printSchema()")
    df_only_pull_events = filter_df(df_raw, 'PullRequestEvent')
    df_only_pull_events.printSchema()

    # df_cc_raw1 = broadcast(df_cc_raw)
    df_only_pull_events.createOrReplaceTempView("df_only_pull_events_tbl")
    int_df_cc_min_max.createOrReplaceTempView("int_df_cc_min_max_tbl")

    focal_df1 = spark.sql(
        "select  e.focal_repo_id, f.repo_id from int_df_cc_min_max_tbl e INNER JOIN df_only_pull_events_tbl f ON e.actor_id == f.actor_id AND (e.focal_repo_id != f.repo_id) AND (f.created_at BETWEEN e.min_d AND e.max_d)")

    # int_df = spark.sql(
    #     "select  f.focal_repo_id, e.repo_id, e.actor_id, e.min_d, e.max_d,f.created_at from df_raw_only_push_events_min_max_dates_tbl e INNER JOIN df_cc_raw_tbl f ON e.actor_id == f.actor_id" )

    # write(int_df,"csv","data/project-temp-out/int_df_data","overwrite")
    # int_df.filter(int_df.focal_repo_id == "112962904").show(20)
    print("focal df contents")
    focal_df1.show(20)
    focal_df1.printSchema()
    # print("count of final: ", int_df.count())
    # print("num of partitionG: ", int_df.rdd.getNumPartitions())
    # int_df1 = int_df.rdd.repartition(12).toDF()
    # print("count of int_df: ", int_df1.count())

    # int_df1 = int_df.drop_duplicates()
    # print("count BEFORE drop duplicates: ", int_df.count())
    # int_df2 = int_df1.drop_duplicates()
    # int_df1.show(50)
    # int_df1.printSchema()
    # int_df1 = int_df.drop_duplicates()
    # print("count after drop duplicates: ", int_df1.count())
    # int_df1.createOrReplaceTempView("int_df1_tbl")
    # final_df = spark.sql(" SELECT focal_repo_id, count(*) as focal_reference from int_df1_tbl group by focal_repo_id ")
    # final_df.show(20)
    # write(final_df,"csv","data/project-temp-out/final_df_data","overwrite")
    # now = datetime.now()
    focal_df = focal_df1.drop_duplicates()
    write(focal_df, "csv", "gs://project_work_files/Output_1GB/focal_df", "overwrite")
    # write(int_df, "csv", "data/project-surr/temp-out/int_df_data-100", "overwrite")

    focal_df_src_dest1 = focal_df.withColumn("source_repo", F.col('focal_repo_id')).withColumn("destination_repo_id",
                                                                                               F.col('repo_id'))
    focal_df_src_dest = focal_df_src_dest1.select("focal_repo_id", "source_repo", "destination_repo_id")

    focal_df_src_dest.show()
    focal_df_src_dest.printSchema()

    write(focal_df_src_dest, "csv", "gs://project_work_files/Output_1GB/focal_df_src_dest", "overwrite")

    focal_df.createOrReplaceTempView("focal_df_tbl")
    focal_df_count = spark.sql(
        "select  focal_repo_id, Count(*) AS count_focal_repo_id from focal_df_tbl group  by focal_repo_id")

    write(focal_df_count, "csv", "gs://project_work_files/Output_1GB/focal_df_count", "overwrite")
    return focal_df_src_dest


def get_i_value(spark, df_repo_raw3, df_cc, df_focal):
    print("Calculate I..")
    # Below code for pull events is used also for focal so can be re-used. remember when executing for BD
    print("df_only_pull_events.printSchema()")
    df_repo_raw3.createOrReplaceTempView("df_repo_raw3_tbl")
    # df_only_pull_events = filter_df(df_repo_raw3, 'PullRequestEvent')
    df_only_pull_events = spark.sql(
        "select distinct repo_id, actor_id from df_repo_raw3_tbl where Type =='PullRequestEvent' GROUP BY repo_id, actor_id")

    print("df_only_pull_events.printSchema()")
    df_only_pull_events.printSchema()

    # df_cc_raw1 = broadcast(df_cc_raw)
    df_only_pull_events.createOrReplaceTempView("df_only_pull_events_tbl")
    #
    #
    # df_focal.createOrReplaceTempView("df_focal_temp")
    # print("df_only_pull_events schema")
    # df_only_pull_events.printSchema()
    # print("df_focal schema")
    # df_focal.printSchema()
    #
    #
    # #need to find out actor id for focal repo id who has done pull events on the same repo id
    # df_focal_only_pull = spark.sql(
    #     "select  f.focal_repo_id, e.actor_id from df_only_pull_events_tbl e INNER JOIN df_focal_temp f ON e.repo_id == f.focal_repo_id ")
    # print("df_focal_only_pull show")
    # #df_focal_only_pull.show(5)
    # write(df_focal_only_pull,"csv","gs://project-work-aug/Output-I/TEMP-OUT-1STQUERY","overwrite")
    #
    # df_focal_only_pull.createOrReplaceTempView('df_focal_only_pull_tbl')
    # #df_cc.drop_duplicates()
    # print("after dropping duplicates of core contributor")
    df_cc12 = broadcast(df_cc)
    df_cc12.createOrReplaceTempView('df_cc_tbl')

    df_focal_only_pull_cc1 = spark.sql(
        "select  DISTINCT f.cc_repo_id AS source_repo_citing_focal_repo, e.actor_id , e.repo_id from df_cc_tbl f  INNER JOIN df_only_pull_events_tbl e ON e.actor_id == f.actor_id AND f.cc_repo_id != e.repo_id ")
    print("df_focal_only_pull_cc1 show")
    # print("count before duplicates df_focal_only_pull_cc1: ",df_focal_only_pull_cc1.count() )

    # df_focal_only_pull_cc = df_focal_only_pull_cc1.drop_duplicates()
    # print("new count after duplicates df_focal_only_pull_cc: ", df_focal_only_pull_cc.count())
    df_focal_only_pull_cc1.show(50)

    print("Rename columns")
    df_focal_only_pull_cc = df_focal_only_pull_cc1.withColumnRenamed("repo_id", "focal_repo_id")
    df_focal_only_pull_cc.printSchema()
    # write(df_focal_only_pull_cc, "csv", "gs://project_work_files/Output_1GB/I_first-out", "overwrite")

    df_focal_only_pull_cc = df_focal_only_pull_cc.withColumn("destination_focal_repo", F.col("focal_repo_id"))
    df_focal_only_pull_cc.printSchema()
    df_rearrage = df_focal_only_pull_cc.select('focal_repo_id', 'source_repo_citing_focal_repo',
                                               'destination_focal_repo')
    print("rearrange columnsdd")
    # write(df_rearrage, "csv", "gs://project_work_files/Output_1GB/I_second-out", "overwrite")
    df_rearrage.createOrReplaceTempView("df_rearrange_tbl")
    print("generating count")
    count_df = spark.sql(" select focal_repo_id, count(*) AS COUNT from df_rearrange_tbl group by focal_repo_id")
    # write(count_df, "csv", "gs://project_work_files/Output_1GB/I_third-out", "overwrite")

    ##Need to calculate actual I
    # FIRST STEP -- Join df_Rearrange with core contributor based on repo id
    print("calculating actual I")
    df_rearrage.createOrReplaceTempView("df_rearrage_tbl")
    df_rearrage_cc_join1 = spark.sql(
        " SELECT e.cc_repo_id AS focal_repo_id, f.source_repo_citing_focal_repo, e.actor_id from df_cc_tbl e INNER JOIN df_rearrage_tbl f ON e.cc_repo_id == f.source_repo_citing_focal_repo")
    df_rearrage_cc_join = df_rearrage_cc_join1.drop_duplicates()
    print("df_rearrage_cc_join schema repo-id")
    ##output 1
    df_rearrage_cc_join.printSchema()

    ##Step 2 -- Output of Focal references join with raw data set only PullEvents on destination repo_id and repo_id
    print("df_focal schema")
    df_focal.printSchema()
    df_focal.createOrReplaceTempView("df_focal_tbl")

    df_focal_only_pull_events_join1 = spark.sql(
        " SELECT e.focal_repo_id, e.destination_repo_id, f.actor_id FROM df_focal_tbl e  INNER JOIN df_only_pull_events_tbl f ON e.destination_repo_id == f.repo_id")
    print("df_focal_only_pull_events_join.printSchema()")
    ##OUTPUT 2
    df_focal_only_pull_events_join = df_focal_only_pull_events_join1.drop_duplicates()
    print("df_focal_only_pull_events_join.printSchema()")
    df_focal_only_pull_events_join.printSchema()
    df_focal_only_pull_events_join.show(10)

    ##JOIN OUTPUT 1 AND OUTPUT 2 on actor id. This is J value
    df_rearrage_cc_join1 = df_rearrage_cc_join.drop_duplicates()
    df_focal_only_pull_events_join1 = df_focal_only_pull_events_join.drop_duplicates()
    df_rearrage_cc_join1.createOrReplaceTempView("df_rearrage_cc_join_tbl")
    df_focal_only_pull_events_join1.createOrReplaceTempView("df_focal_only_pull_events_join_tbl")
    print("after dropping duplicates")

    df_join_act = spark.sql(
        " SELECT e.focal_repo_id, e.source_repo_citing_focal_repo, f.destination_repo_id  from df_rearrage_cc_join_tbl e INNER JOIN df_focal_only_pull_events_join_tbl f ON e.actor_id == f.actor_id")
    df_join = df_join_act.drop_duplicates()
    print("df_join show")
    df_join.show(10)
    write(df_join, "csv", "gs://project_work_files/Output_1GB_7/J_Value", "overwrite")
    df_join.createOrReplaceTempView("df_join_tbl")
    df_count_j = spark.sql("SELECT focal_repo_id, COUNT(*) as J_Count FROM df_join_tbl GROUP BY focal_repo_id")
    write(df_count_j, "csv", "gs://project_work_files/Output_1GB_7/J_Value_Count", "overwrite")

    # last step to calculate actual I . we need to subtract from j
    df_actual_i1 = spark.sql(
        "select e.focal_repo_id, e.source_repo_citing_focal_repo, e.destination_repo_id, f.focal_repo_id from df_rearrage_tbl e LEFT JOIN df_join_tbl f ON e.source_repo_citing_focal_repo == f.source_repo_citing_focal_repo WHERE f.focal_repo_id is NULL")
    print("df_actual_i printSchema")
    df_actual_i = df_actual_i1.drop_duplicates()
    df_actual_i.printSchema()
    df_actual_i.show(30)
    write(df_actual_i, "csv", "gs://project_work_files/Output_1GB_7/Actual_I_Value", "overwrite")
    df_actual_i.createOrReplaceTempView("df_actual_i_tbl")
    df_actual_i_count = spark.sql("select focal_repo_id, count(*) as count from df_actual_i_tbl GROUP BY focal_repo_id")
    write(df_actual_i_count, "csv", "gs://project_work_files/Output_1GB_7/Actual_I_Value_Count", "overwrite")
    print("DONE....ENJOY!!")

    return df_join


def outer_join_test(spark):
    print("inside outer join test")
    df_1 = read_input(spark, "data/employee1.csv", "|", "csv", "True")
    df_1.show()
    df_2 = read_input(spark, "data/dept.csv", "|", "csv", "true")
    df_2.show()
    df = df_2.join(df_1, on=['dept_id'], how='left_anti')
    print("after anti join")
    df.show(5)
    exit(0)


def main():
    print("Here you go!!")
    now = datetime.now()

    print("START OF PROGRAM date and time = ", now)
    # removeConstUDF("2016-01-15 18:39:47 UTC")

    spark, sc = init_spark()
    # outer_join_test(spark)

    # df_repo_raw = read_input(spark, 'data/githubrepo_dataset000000000000.csv', ',', "csv","true")
    # df_repo_raw = read_input(spark, 'data/project-surr/input-100', ',', "csv", "true")
    # df_repo_raw = read_input(spark, 'gs://githubdataset_files/', ',', "csv", "true")
    df_repo_raw = read_input(spark, 'gs://project_work_files/Input_1GB/githubrepo_dataset000000000000.csv', ',', "csv",
                             "true")
    # df_repo_raw = read_input(spark, 'gs://project-work-aug/project-surr/input/github-partaa.csv', ',', "csv", "true")
    # print("Read baseline file WITH COUNT: " ,df_repo_raw.count())
    df_repo_raw1 = df_repo_raw.drop_duplicates()

    # df_repo_raw.show(truncate=False)
    # df_repo_raw.printSchema()
    # df_repo_raw1 = df_repo_raw.unique().show()
    # exit(0)

    df_repo_raw2 = df_repo_raw1.select(F.col("repo_id"), F.col("repo_name"), F.col("actor_id"), F.col("Type"),
                                       F.substring(F.col("created_at"), 0, 19).alias("updated_timestamp"))
    # df_repo_raw2.show(truncate=False)
    df_repo_raw2.printSchema()
    df_repo_raw3 = df_repo_raw2.withColumn('created_at',
                                           F.to_timestamp(F.col('updated_timestamp'), "yyyy-MM-dd HH:mm:ss")).drop(
        F.col("updated_timestamp"))
    # df_repo_raw3.show(5)
    df_repo_raw3.printSchema()

    print("HERE with gcp")
    # df_new = df.withColumn("value_desc", F.when(df.actor_id == 10905263  , df.repo_name).when(df.actor_id == 1222986, df.actor_id).otherwise('other'))

    # df_new = df_repo_raw3.withColumn("new_column",(F.when(F.col("actor_id") == 10905263 , F.col("repo_name"))
    #                                    .F.when(F.col("actor_id") == 1222986 , F.col("repo_name"))
    #                                            .F.when(F.col("actor_id") == 14191903, F.col("repo_id") )))

    # uncomment the following code
    df_core_contributors = get_core_contributors(spark, df_repo_raw3)
    # df_core_contributors.printSchema()
    # write(df_core_contributors,"csv","data/project-temp-out/core-cont-list","overwrite")
    # write(df_core_contributors,"csv","data/project-surr/temp-out/core-cont-list-200","overwrite")

    # test code for reading the cc list from file instead of calculating
    # print("ddNumber of raw input partitions before: ", df_repo_raw3.rdd.getNumPartitions())
    # df_repo_raw4 = df_repo_raw3.rdd.repartition(12).toDF()
    # print("Number of raw input partitions: ", df_repo_raw4.rdd.getNumPartitions())

    # schema_cc = StructType([
    #     StructField("cc_repo_id", StringType(), False), StructField("actor_id", StringType(), False)
    # ])

    # df_core_contributors = read_test(spark,schema_cc,"data/project-temp-out/core-cont-list")

    # df_core_contributors = read_test(spark, schema_cc, "gs://project-work-aug/Output-CC")
    # df_core_contributors = read_input(spark,"gs://project-work-aug/Output-CC",",","csv","True")
    # df_core_contributors1 = df_core_contributors.drop_duplicates()
    # write(df_core_contributors1, "csv", "gs://project-work-aug/Output-CC", "overwrite")

    # print("Number of df_core_contributors1 partitions before: ", df_core_contributors1.rdd.getNumPartitions())
    # df_core_contributors2 = df_core_contributors1.rdd.repartition(12).toDF()
    # print("Number of df_core_contributors1 partitions after: ", df_core_contributors2.rdd.getNumPartitions())
    df_core_contributors1 = df_core_contributors.withColumnRenamed("repo_id", "cc_repo_id")
    print("df_core_contributors")
    # print("count before df_core_contributors dup: ", df_core_contributors.count())
    # df_core_contributors1 = df_core_contributors.drop_duplicates()
    # print("count after df_core_contributors dup: ", df_core_contributors1.count())

    df_core_contributors1.show(5)
    df_core_contributors1.printSchema()
    # write(df_core_contributors1, "csv", "gs://project_work_files/Output_1GB/Output-CC", "overwrite")

    # fdf = get_focal_references(spark, df_core_contributors1,df_repo_raw3)
    # fdf = read_input(spark,"data/project-surr/focal-ref-out",",","csv","True")
    fdf = read_input(spark, "gs://project_work_files/Output_1GB/focal_df_src_dest", ",", "csv", "True")
    # print("count of focal: ", fdf.count())

    print("showing focal df read from storage")
    fdf.show(5)
    fdf.printSchema()
    get_i_value(spark, df_repo_raw3, df_core_contributors1, fdf)
    now = datetime.now()
    print("END OF PROGRAM date and time . all done=", now)
    spark.stop()
    # exit(0)


if __name__ == '__main__':
    main()