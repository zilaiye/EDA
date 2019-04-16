# EDA
explorementory analysis is a basic process before we create a model on some data , unlike other tools such a python or R language which are good at small data scale  ,spark can scale to a large amount of data , but  not capable to do this job in limit time , so I create this program to solve this problem , this project works pretty good in financial area and makes our team more productive ,usually EDA task can be finished in 15 minutes . 

# basic solution for parallel compute index on a DataFrame 
when we run a describe on a DataFrame , we can hardly bear the time spend on this task for spark did this one row after another , and as the data become more and more diversity , the columns will be more and more , our team is facing a 1000000 * 12000 data , so to solve this problem , we have to transform the DataFrame to be a 12000*100000 , so we can do this task parallel through each row , it's really awesome .

# usage example 
val spark = SparkSession.builder()
        .config("spark.dynamicAllocation.maxExecutors","30")
        .config("spark.dynamicAllocation.minExecutors","20")
        .config("spark.executor.memory","8g")
        .config("spark.sql.adaptive.enabled","True")
        .config("spark.default.parallelism", 512)
        .config("spark.executor.cores",4)
        .appName("EDA-Test")
        .enableHiveSupport()
        .getOrCreate()

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import scala.util.Sorting.quickSort

import com.kylin.skywork.util._
import com.kylin.skywork.EDA

// load the data 
val all_data = spark.read.option("header", "true").csv("hdfs:///path/to/data")
// get the column names 
val names = all_data.schema.fields.map(f => f.name)
// transform dataframe to double 
val double_df = all_data.select(names.map(name => column(name).cast(DoubleType)): _*) 
// split dataframe to train and test , ignore ....
// transfrom train_df (1000000*10000 => 10000*1000000)
val rdd_train = DataTransform.cols_2_rows(train_df,true)
val total_count = train_df.count()

// get the desc info for train_df , just like the pandas.describe() function 
val desc_train = EDA.describe_rdd(rdd_train,total_count,names)

// caculate entropy 
val entropy_values =  Entropy.compute_entropy(rdd_train,total_count)

// transform test_df  (1000000*10000 => 10000*1000000)
val rdd_test = DataTransform.cols_2_rows(test_df,true)

// calculate the psi 
val rdd_psi = PSI.calculate_psi_rdd(rdd_train,rdd_test,ncols)

// then the min , max , sum , avg , var , dev ,quantiles , entropy , psi , you can then collect them as a DataFrame
val selected_field_names = Array(... your columns list here ... )
val desc_filtered_fields = desc_train.filter(e => selected_field_names.toSet.contains(e._1))
val df_desc = spark.sparkContext.parallelize(desc_filtered_fields).toDF("feature_name",
                                                              "max","min","sum",
                                                              "count","missing_rate","variance",
                                                              "stddev","median","mean","percent25","percent75")
// now you can select which columns should be reserved for model ,just do some filter work on this dataframe to choose which you truly need 