package broadcast.hdfs;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WriteHDFS {
    private static final Logger LOG = LoggerFactory.getLogger(WriteHDFS.class);
    public static void main(String[] args) {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(executionEnvironment);
        streamTableEnvironment.executeSql("CREATE TABLE datagen_source_table ( \n" +
                "    id INT, \n" +
                "    name STRING \n" +
                ") WITH ( \n" +
                "    'connector' = 'datagen',\n" +
                "    'rows-per-second'='1'  -- 每秒产生的数据条数\n" +
                ")");
        streamTableEnvironment.executeSql("CREATE TABLE filesystem (\n" +
                "  rowkey STRING,\n" +
                "  info ROW < id int,name STRING >,\n" +
                "  PRIMARY KEY (rowkey) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'filesystem',                     \n" +
                "  'path' = 'hdfs://172.16.48.124:4007/tmp/ybx-text',             \n" +
                "  'format' = 'json' \n" +
                ")");
        LOG.info("insert into");
        streamTableEnvironment.executeSql("insert into filesystem select cast(id as string),ROW(id,name) as info from datagen_source_table")
                .print();

    }
}
