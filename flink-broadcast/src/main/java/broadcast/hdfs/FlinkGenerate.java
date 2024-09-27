package broadcast.hdfs;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.StreamEnv;


public class FlinkGenerate {
    private static Logger LOG = LoggerFactory.getLogger(FlinkGenerate.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamEnv.getInstance();

        environment.enableCheckpointing(30000);
        environment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        environment.getCheckpointConfig().setMinPauseBetweenCheckpoints(500); //设置ck之间的间隔时间
        environment.getCheckpointConfig().setCheckpointTimeout(60000); //ck的超市时间
        environment.getCheckpointConfig().setMaxConcurrentCheckpoints(1); //同一时间只是允许一个ck进行
        //在job 中止后继续保留ck
        environment.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        environment.setParallelism(6);
        DataStreamSource<Long> source = environment.generateSequence(1, 1_000_000L);
        SingleOutputStreamOperator<Double> streamOperator = source.map(new MapFunction<Long, Double>() {
            @Override
            public Double map(Long value) throws Exception {
                // 计算数据的开立方根，cos
                double cbrt = Math.cos(Math.cbrt(value));
                LOG.info("开立方，在平方结果： {}", cbrt);
                return cbrt;
            }
        });

        // 对结果数据进行排序
        SingleOutputStreamOperator<Double> sortResult = streamOperator.map(new MapFunction<Double, Tuple2<Double, Double>>() {
            @Override
            public Tuple2<Double, Double> map(Double value) throws Exception {
                return Tuple2.of(value, value);
            }
        }).keyBy(0).sum(1).map(new MapFunction<Tuple2<Double, Double>, Double>() {
            @Override
            public Double map(Tuple2<Double, Double> value) throws Exception {
                return value.f0;
            }
        });

        sortResult.print().name("sort-out");
        // 执行作业并测量吞吐量和延迟
        long startTime = System.currentTimeMillis();
        environment.execute();
        long endTime = System.currentTimeMillis();

        // 计算总延迟和吞吐量
        long totalDelay = endTime - startTime;
        double throughput = (double) 10000000 / (totalDelay / 1000.0);
        System.out.println("================generate report========================");
        System.out.println("Total Delay: " + totalDelay + " ms");
        System.out.println("Throughput: " + throughput + " records/second");
        System.out.println("====================end================================");
//        environment.execute("generate");
    }

}

