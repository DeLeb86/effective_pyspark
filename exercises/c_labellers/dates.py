import datetime
from datetime import date,datetime
import time
from pyspark.sql import functions as sf
from pyspark.sql import DataFrame
from pyspark.sql.types import  BooleanType
import holidays
BE_HOLIDAYS=holidays.BE(years=range(1990,datetime.now().year))
HOLIDATES=[d.strftime("%Y-%m-%d") for d,n in BE_HOLIDAYS.items()]

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
    pass
