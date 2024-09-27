package broadcast.hdfs;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import util.StreamEnv;

public class FLinkMySinkHdfs {
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamEnv.getInstance();

        environment.setParallelism(1);

        DataStreamSource<String> source = environment.fromElements("hello", "world");


        source.addSink(new MyhdfSink());



    }
}
