package broadcast.hbase;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FLinkTableWriteHbase {
    private static Logger LOG = LoggerFactory.getLogger(FLinkTableWriteHbase.class);

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
        streamTableEnvironment.executeSql("CREATE TABLE dim_hbase (\n" +
                "  rowkey STRING,\n" +
                "  info ROW < id int,name STRING >,\n" +
                "  PRIMARY KEY (rowkey) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'hbase-2.2',                       \n" +
                "  'table-name' = 'gd_test_2',                      \n" +
                "  'zookeeper.quorum' = '172.16.48.124:2181,172.16.48.27:2181,172.16.48.15:2181', \n" +
                "  'zookeeper.znode.parent'= '/hbase-tbds-r3yeh8a9' \n" +
                ")");
        streamTableEnvironment.executeSql("insert into dim_hbase select cast(id as string),ROW(id,name) as info from datagen_source_table").print();
    }
}
