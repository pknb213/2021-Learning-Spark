from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.flume import FlumeUtils

flumeStream = FlumeUtils.createStream(streamingContext, [chosen machine's hostname], [chosen port])

sc = SparkContext(master, appName)
ssc = StreamingContext(sc, 1)
streamingContext.textFileStream(dataDirectory)
