package window;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/*
统计在最近的5条信息中汽车的数量， 相同的key每出现5次统计一次

 */
public class CountWindow1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.setParallelism(1);
        DataStreamSource<String> streamSource = environment.socketTextStream("localhost", 7819);

        SingleOutputStreamOperator<CarInfo> map = streamSource.map(new MapFunction<String, CarInfo>() {
            @Override
            public CarInfo map(String value) throws Exception {
                String[] strings = value.split(",");
                return new CarInfo(strings[0], Integer.parseInt(strings[1]));
            }
        });
        // 表示在最近的5条消息中各个红绿灯经过的汽车数量
        SingleOutputStreamOperator<CarInfo> count = map.keyBy(CarInfo::getSensorId).countWindow(5L).sum("count");
        map.keyBy(CarInfo::getSensorId).countWindow(5L).process(new ProcessWindowFunction<CarInfo, Integer, String, GlobalWindow>() {

            /**
             * 将当前窗口中数据的汽车总量计算，并发送到下游
             * @param s The key for which this window is evaluated.
             * @param context The context in which the window is being evaluated.
             * @param elements The elements in the window being evaluated.
             * @param out A collector for emitting elements.
             * @throws Exception
             */
            @Override
            public void process(String s, ProcessWindowFunction<CarInfo, Integer, String, GlobalWindow>.Context context, Iterable<CarInfo> elements, Collector<Integer> out) throws Exception {
                // 判断之前的数据中是否包含当前key的数据，如果有的话，获取到在更新这个key的value
                int sum = 0;
                for (CarInfo e: elements
                     ) {
                    sum += e.getCount();
                }
                out.collect(sum);
            }
        }).print();

        //表示在最近的5条信息中统计相同红绿灯编号中每出现3次就要进行统计一次
//        SingleOutputStreamOperator<CarInfo> count = map.keyBy(CarInfo::getSensorid).countWindow(5L, 3L).sum("count");
//        count.print();

//        count.print();
        environment.execute("countWindow");

    }





    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class CarInfo{
        private String sensorId;
        private int count;

        public String toString() {
            return "carInfo-> (sensorId: " + sensorId + ","+ count + ")";
        }

    }
}
