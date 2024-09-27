package broadcast.hbase;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import util.StreamEnv;

import java.nio.charset.StandardCharsets;

public class FlinkWriteHbase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamEnv.getInstance();
        environment.disableOperatorChaining();
        String zkHost = args[0];
        String hbaseTableName = args[0];

        DataStreamSource<String> source = environment.fromElements("hello", "world");
        source.addSink(new RichSinkFunction<String>() {
            org.apache.hadoop.conf.Configuration configuration;
            Connection connection;
            @Override
            public void open(Configuration parameters) throws Exception {
                 configuration = HBaseConfiguration.create();
                 configuration.set("habse.zookeeper.quorum", zkHost);
                 configuration.set("zookeeper.znode.parent","/hbase-tbds-kemb45js");
                 connection = ConnectionFactory.createConnection(configuration);
            }
            @Override
            public void invoke(String value, Context context) throws Exception {
                Table table = connection.getTable(TableName.valueOf(hbaseTableName));
                Put put = new Put("rowkey".getBytes(StandardCharsets.UTF_8));
                put.addColumn("info".getBytes(StandardCharsets.UTF_8), "name".getBytes(StandardCharsets.UTF_8),
                        value.getBytes(StandardCharsets.UTF_8));
                table.put(put);
                table.close();
            }
            @Override
            public void close() throws Exception {
                super.close();
                connection.close();
            }
        });

        environment.execute("hbase-test");

    }
}
