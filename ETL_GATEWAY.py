import os
import pandas as pd
import pyspark
from pyspark.sql import SparkSession

from UCL_Analysis.ETL.load import load
from UCL_Analysis.ETL.normalize import normalize
from UCL_Analysis.ETL.extract import extract

extract = extract()
extract.extract()

normalize = normalize()
normalize.normalize()

load = load()
load.load()

