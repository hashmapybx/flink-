package broadcast.hive;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FlinkDSHive {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkDSHive.class);
    public static void main(String[] args) throws Exception {
        if (args.length !=2) {
            LOG.info("input hive database name, hive cnf path");
            System.exit(-1);
        }
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(executionEnvironment);
        String catalogName = "test_flink";
        String database = args[0];
        String hiveConfDir = args[1];
        HiveCatalog hiveCatalog = new HiveCatalog(catalogName, database, hiveConfDir);
        streamTableEnvironment.registerCatalog("hive_catalog",hiveCatalog);
        streamTableEnvironment.useCatalog("hive_catalog");
        streamTableEnvironment.getConfig().setSqlDialect(SqlDialect.HIVE);
        streamTableEnvironment.useDatabase("ybx");
        // 将datastream写入 hive表
        // 创建输入表

        DataStreamSource<Tuple2<Integer, String>> streamSource = executionEnvironment.addSource(new RichSourceFunction<Tuple2<Integer, String>>() {

            @Override
            public void run(SourceContext<Tuple2<Integer, String>> ctx) throws Exception {

            }

            @Override
            public void cancel() {

            }
        });


        Table inputTable = streamTableEnvironment.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("age", DataTypes.INT())
                ),
                Row.of("Alice", 25),
                Row.of("Bob", 30),
                Row.of("Charlie", 35)
        );
        String tableName = "myTable";
        String createTableDDL = "CREATE TABLE IF NOT EXISTS " + tableName + " (name STRING, age INT) " +
                "PARTITIONED BY (dt STRING) " +
                "STORED AS PARQUET TBLPROPERTIES (\n"+
                "'sink.partition-commit.policy.kind' = 'metastore,success-file'  \n" +
                ");";
        streamTableEnvironment.executeSql(createTableDDL);
        // 将表数据写入 Hive 表
        String insertIntoHiveTable = "INSERT INTO " + tableName + " PARTITION (dt='2022-01-01') " +
                "SELECT name, age FROM " + inputTable;
        streamTableEnvironment.executeSql(insertIntoHiveTable);
        // 执行作业
//        executionEnvironment.execute();

    }
}
