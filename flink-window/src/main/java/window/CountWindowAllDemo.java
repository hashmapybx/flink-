package window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * countWindow
 */
public class CountWindowAllDemo {
    private static final Logger logger = LoggerFactory.getLogger(CountWindowAllDemo.class);
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.setParallelism(1);
        DataStreamSource<String> streamSource = environment.socketTextStream("localhost", 7819);

        SingleOutputStreamOperator<CountWindow1.CarInfo> map = streamSource.map(new MapFunction<String, CountWindow1.CarInfo>() {
            @Override
            public CountWindow1.CarInfo map(String value) throws Exception {
                String[] strings = value.split(",");
                return new CountWindow1.CarInfo(strings[0], Integer.parseInt(strings[1]));
            }
        });

        map.keyBy(CountWindow1.CarInfo::getSensorId).countWindowAll(5L).sum("count").print().name("print");

        try {

            environment.execute("print");
        }catch (Exception e) {
            logger.error(e.getMessage());
        }
    }
}
