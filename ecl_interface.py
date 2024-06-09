from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import *
import csv
from abc import ABC, abstractmethod
from pyspark.sql.types import IntegerType, DecimalType

def discount_factor(int_real_rate: float, period:int):
    return 1 / (1 + (int_real_rate/100)/12) ** period
class IWorkspace(ABC):
    spark = SparkSession.builder

class SQLView(IWorkspace):
    spark = SparkSession.builder.appName("LGD Workout Period Analysis").enableHiveSupport().getOrCreate()

    def __init__(self, temp_table: str):
        self.sql = self.spark.sql("SELECT * FROM {}".format(temp_table))

    @property
    def result(self):
        return self.sql.collect()

    @result.getter
    def result(self):
        return self.sql.collect()

class WorkoutAnalysis(IWorkspace):
    spark = SparkSession.builder.appName("LGD Workout Period Analysis").enableHiveSupport().getOrCreate()
    
    def __init__(self):
        self.df = self.spark.table("ecl.rep_fin_ecl_lgd_default_population_ss_m")
        self.df_movement = self.df.select(to_date("pt_date").alias("pt_date"), "loan_no", "pd_segment", "tenor", "first_default_date","first_default_principal","cur_balance", "cur_balance_mom", "int_real_rate", months_between(to_date("pt_date"), last_day(to_date("first_default_date"))).alias("period"))
        
        self.run_recovery()

    def run_recovery(self):
        df_movement = self.df_movement
        df_movement = df_movement.withColumn("recovery_amount", nanvl(df_movement['cur_balance'],lit(0)) - nanvl(df_movement['cur_balance_mom'],lit(0)))
        df_movement = df_movement.withColumn("discount_factor", discount_factor(df_movement['int_real_rate'], df_movement['period']))
        df_movement = df_movement.withColumn("pv_recovery_amount", (df_movement['cur_balance'] - df_movement['cur_balance_mom']) * discount_factor(df_movement['int_real_rate'], df_movement['period']))
        df_movement = df_movement.withColumn("recovery_rate", df_movement['recovery_amount'] / df_movement['first_default_principal'])
        df_movement = df_movement.withColumn("pv_recovery_rate", df_movement['pv_recovery_amount'] / df_movement['first_default_principal'])

        self.df_movement = df_movement
    
    @property
    def result(self):
        return self.df_movement

    @result.getter
    def result(self):
        return self.df_movement

    @property
    def to_sql(self):
        return self.df_movement

    @to_sql.getter
    def to_sql(self):
        return self.df_movement
class PDCohort(IWorkspace):
    spark = SparkSession.builder.appName("PD Cohort").enableHiveSupport().getOrCreate()

    def __init__(self):
        ...

class TransitionMatrix(IWorkspace):
    spark = SparkSession.builder.appName("PD Transition Matrix").enableHiveSupport().getOrCreate()

    def __init__(self):
        ...

if __name__ == '__main__':
    df = WorkoutAnalysis()
    df.result.orderBy(['loan_no', 'period']).groupBy('pd_segment').agg(sum('recovery_amount').cast(DecimalType()).alias('recovery_amount'),sum('pv_recovery_amount').cast(DecimalType()).alias('pv_recovery_amount')).createOrReplaceTempView('lgd_workout_analysis')
    