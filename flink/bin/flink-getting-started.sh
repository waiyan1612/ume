FLINK_BIN=/usr/local/Cellar/apache-flink/1.7.2/libexec/bin

# Start flink cluster
sh $FLINK_BIN/start-cluster.sh

# Stop flink cluster
sh $FLINK_BIN/stop-cluster.sh

# Start flink shell
sh $FLINK_BIN/start-scala-shell.sh local
sh $FLINK_BIN/start-scala-shell.sh remote localhost 6123

# Submit job
sh $FLINK_BIN/flink run flink-assembly*.jar com.waiyan.ume.flink.api.Producer --port 9000