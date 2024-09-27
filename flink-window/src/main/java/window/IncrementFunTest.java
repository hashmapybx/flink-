package window;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * 关于增量聚合参数的操作
 * aggregate function reduction
 *
 */
public class IncrementFunTest {
    private static final Logger logger = LoggerFactory.getLogger(IncrementFunTest.class);
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.setParallelism(1);

        // 数据源产生传感器采集数据
        DataStreamSource<SensorReading> streamSource = environment.addSource(new RichSourceFunction<SensorReading>() {
            private boolean isRunning = true;

            Random random = new Random();

            @Override
            public void run(SourceContext<SensorReading> ctx) throws Exception {

                while (isRunning) {

                    String id = "远洋-1" + random.nextInt(10);
                    long val1 = random.nextLong();
                    double temperature = Math.round(random.nextGaussian()) * 100;
                    SensorReading sensorReading = new SensorReading(id, val1, temperature);
                    ctx.collect(sensorReading);
                    TimeUnit.SECONDS.sleep(1);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });

        //现在是一个增量聚合操作
        SingleOutputStreamOperator<Integer> resultDS = streamSource.keyBy(SensorReading::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(SensorReading value, Integer accumulator) {
                        return accumulator +1;
                    }

                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return a+b;
                    }
                });


        resultDS.print();
        try {
            environment.execute("incre");
        }catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class SensorReading {
        private String id;
        private long val1;
        private double temperature;
    }

}
