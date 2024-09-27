package broadcast.tran;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.StreamEnv;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Random;

public class TransformTest {
    private static final Logger LOG = LoggerFactory.getLogger(TransformTest.class);
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamEnv.getInstance();
        // 自定义数据源
        DataStreamSource<Tuple3<Integer, String, Double>> source = environment.addSource(new SourceFunction<Tuple3<Integer, String, Double>>() {
            private boolean isRunning = true;
            @Override
            public void run(SourceContext<Tuple3<Integer, String, Double>> ctx) throws Exception {
                Random random = new Random();
                int id = 1;
                while (isRunning) {
                    String name = "a_" + random.nextInt(10);
                    double score = random.nextGaussian() * 100 + random.nextInt(100);
                    ctx.collect(Tuple3.of(id, name, score));
                    id++;
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });


        SingleOutputStreamOperator<Tuple3<Integer, String, Double>> filter = source.filter(new FilterFunction<Tuple3<Integer, String, Double>>() {
            @Override
            public boolean filter(Tuple3<Integer, String, Double> value) throws Exception {
                return value.f2 > 101;
            }
        });


        SingleOutputStreamOperator<Tuple3<Integer, String, Double>> sum = filter.keyBy(f -> f.f1).sum(2);

        sum.print();

        // 将结果写入mysql
        sum.addSink(new RichSinkFunction<Tuple3<Integer, String, Double>>() {
            Connection connection = null;
            PreparedStatement ps = null;

            String url = "jdbc:mysql://106.52.213.250:3306/bt_pocl?useUnicode=true&useSSL=false";

            String username = "root";
            String passwd = "Tbds@12345";

            @Override
            public void open(Configuration parameters) throws Exception {
                connection = DriverManager.getConnection(url, username, passwd);
                connection.setAutoCommit(false);
            }


            @Override
            public void close() throws Exception {
                if (connection != null) connection.close();
                if (ps != null) ps.close();
            }

            @Override
            public void invoke(Tuple3<Integer, String, Double> value, Context context) throws Exception {
                String sql = "insert into s2(id, name, scores) values (?,?,?);";
                ps = connection.prepareStatement(sql);
                ps.setInt(1, value.f0);
                ps.setString(2, value.f1);
                ps.setDouble(3, value.f2);
                ps.execute();
                connection.commit();
                LOG.info("commit success:{}", value.toString());
            }
        });


        try {
            environment.execute("trans");
        }catch (Exception e) {
            LOG.error("failed, {}", e.getMessage());
        }


    }



}
