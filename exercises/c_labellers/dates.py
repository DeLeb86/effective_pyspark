import datetime
from datetime import date,datetime
import time

from pyspark.sql import functions as sf,SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import  BooleanType,StructField,StringType,StructType,DateType
import holidays
BE_HOLIDAYS=holidays.BE(years=range(1990,datetime.now().year))
HOLIDATES=[d for d,n in BE_HOLIDAYS.items()]

spark=SparkSession.builder.getOrCreate()
fields=[StructField("timestamp",DateType(),nullable=False),
        StructField("holiday",StringType(),nullable=False)]

def is_belgian_holiday(date: datetime.date) -> bool:
    return date.isoformat() in BE_HOLIDAYS
    


def label_weekend(
    frame: DataFrame, colname: str = "date", new_colname: str = "is_weekend"
) -> DataFrame:
    return frame.withColumn(new_colname, ((sf.dayofweek(frame[colname]) == 1) | (sf.dayofweek(frame[colname]) == 7)))
    """Adds a column indicating whether or not the attribute `colname`
    in the corresponding row is a weekend day."""


def label_holidays(
    frame: DataFrame,
    colname: str = "date",
    new_colname: str = "is_belgian_holiday",
) -> DataFrame:
    return frame.withColumn(new_colname,sf.col(colname).isin(HOLIDATES))
    df_timestamp=frame.withColumn("timestamp",sf.to_date(colname,'yyyy-MM-dd'))
    frame=df_timestamp.withColumn(new_colname,sf.col("timestamp").isin(HOLIDATES))
    return frame.drop("timestamp")
    
    """Adds a column indicating whether or not the column `colname`
    is a holiday."""
    pass


def label_holidays2(
    frame: DataFrame,
    colname: str = "date",
    new_colname: str = "is_belgian_holiday",
) -> DataFrame:
    
    """Adds a column indicating whether or not the column `colname`
    is a holiday. An alternative implementation."""
    is_holidays=sf.udf(is_belgian_holiday,returnType=BooleanType())
    return frame.withColumn(new_colname,is_holidays(colname))
    
    


def label_holidays3(
    frame: DataFrame,
    colname: str = "date",
    new_colname: str = "is_belgian_holiday",
) -> DataFrame:
    """Adds a column indicating whether or not the column `colname`
    is a holiday. An alternative implementation."""
    df_holidays=spark.createDataFrame(
        data=BE_HOLIDAYS.items(),
        schema=StructType(fields)
    )
    joined=frame.join(df_holidays,frame["date"]==df_holidays["timestamp"],"left_outer")
    frame=joined.withColumn(new_colname,sf.when(joined["date"].isNull(),None).when(joined["timestamp"].isNull(),False).otherwise(True))
    frame=frame.drop("timestamp","holiday")
    return frame
    