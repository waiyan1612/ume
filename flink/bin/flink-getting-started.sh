FLINK_BIN=/usr/local/Cellar/apache-flink/1.7.2/libexec/bin

# Start flink cluster
sh $FLINK_BIN/start-cluster.sh

# Stop flink cluster
sh $FLINK_BIN/stop-cluster.sh

# Start flink shell
sh $FLINK_BIN/start-scala-shell.sh remote localhost  6123

val dataSet = benv.readTextFile("/Users/dataspark/Downloads/carparks.csv")

# Submit job
sh $FLINK_BIN/flink run flink-assembly*.jar com.waiyan.ume.flink.BatchJob --port 9000