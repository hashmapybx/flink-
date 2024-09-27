package window;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/*
测试增量的reduce 操作

 */
public class IncrementFun2 {
    private static final Logger logger = LoggerFactory.getLogger(IncrementFun2.class);
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.setParallelism(1);
        DataStreamSource<SensorReading> streamSource = environment.addSource(new RichSourceFunction<SensorReading>() {
            private boolean isRunning = true;

            Random random = new Random();

            @Override
            public void run(SourceContext<SensorReading> ctx) throws Exception {

                while (isRunning) {

                    String id = "远洋-1" + random.nextInt(10);
                    long val1 = random.nextLong();
                    double temperature = random.nextGaussian() * 100;
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
        streamSource.keyBy(SensorReading::getId).window(TumblingProcessingTimeWindows.of(Time.seconds(10))).reduce(new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {

                double temperature1 = value1.getTemperature();
                double temperature2 = value2.getTemperature();

                if (temperature1 > temperature2) {
                    return value2;
                }else {
                    return value1;
                }
            }
        }).print();

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
