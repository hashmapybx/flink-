package window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * sessionwindow  测试
 */
public class SessionWindowDemo {
    private static final Logger logger = LoggerFactory.getLogger(SessionWindowDemo.class);
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

        //每5秒钟统计一次，统计最近5秒内，各个红绿灯路口通过的汽车的数量，基于时间的滚动窗口

        //session设置的会话时间5秒到了就会触发窗口的计算
        map.keyBy(CountWindow1.CarInfo::getSensorId)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
                .sum("count").print();


        // 每5秒钟统计一次，统计最近10秒内，各个红绿灯路口通过的汽车的数量，基于时间的滑动窗口

//        map.keyBy(CountWindow1.CarInfo::getSensorId).window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
//                .sum("count").print();

        try {
            environment.execute("session");

        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }
}
