package broadcast;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import util.StreamEnv;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Random;

public class UserInfoBroadCastStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamEnv.getInstance();

        environment.setParallelism(1);

        // 构件随机的用户日志实时流 <userid, eventTime, eventType. productId>
        DataStreamSource<Tuple4<String, String, String, Integer>> eventSource = environment.addSource(new SourceFunction<Tuple4<String, String, String, Integer>>() {
            private boolean isRunning = true;

            @Override
            public void run(SourceContext<Tuple4<String, String, String, Integer>> ctx) throws Exception {
                Random random = new Random();
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                while (isRunning) {
                    int id = random.nextInt(4)+1;
                    String user_id = "user_" + id;
                    String evenTime = df.format(new Date());
                    String evenType = "type_" + random.nextInt(3);
                    int productId = random.nextInt(4);
                    ctx.collect(Tuple4.of(user_id, evenTime, evenType, productId));
                    Thread.sleep(500);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });

        //构件配置流 <userId, userName, userAge>

        DataStreamSource<Map<String, Tuple2<String, Integer>>> configDS = environment.addSource(new MysqlSource());

        // 转换操作
        // 先定义状态转换描述器
        MapStateDescriptor<Void, Map<String, Tuple2<String, Integer>>> descriptor =
                new MapStateDescriptor<>("config", Types.VOID, Types.MAP(Types.STRING, Types.TUPLE(Types.STRING, Types.INT)));

        // 广播配置流 将配置流根据状态描述器广播出去,变成广播状态流
        BroadcastStream<Map<String, Tuple2<String, Integer>>> broadcastStream = configDS.broadcast(descriptor);
        //-3.将事件流和广播流进行连接
        BroadcastConnectedStream<Tuple4<String, String, String, Integer>, Map<String, Tuple2<String, Integer>>> connect = eventSource.connect(broadcastStream);

        //  处理连接后的流-根据配置流补全事件流中的用户的信息

        SingleOutputStreamOperator<Tuple6<String, String, String, Integer, String, Integer>> result = connect.process(new BroadcastProcessFunction<
                Tuple4<String, String, String, Integer>,  //事件流
                Map<String, Tuple2<String, Integer>>, // 规则流
                // <用户id，eventTime，eventType，productID，姓名，年龄> //需要收集的数据
                Tuple6<String, String, String, Integer, String, Integer>>() {


            // 处理事件流
            @Override
            public void processElement(Tuple4<String, String, String, Integer> value, BroadcastProcessFunction<Tuple4<String, String, String, Integer>, Map<String, Tuple2<String, Integer>>, Tuple6<String, String, String, Integer, String, Integer>>.ReadOnlyContext ctx, Collector<Tuple6<String, String, String, Integer, String, Integer>> out) throws Exception {
                // 取出来userID
                String userId = value.f0;
                // 根据状态描述器广播
                ReadOnlyBroadcastState<Void, Map<String, Tuple2<String, Integer>>> broadcastState = ctx.getBroadcastState(descriptor);
                if (broadcastState != null) {
                    Map<String, Tuple2<String, Integer>> map = broadcastState.get(null);
                    if (map != null) {
                        Tuple2<String, Integer> tuple2 = map.get(userId);
                        String username = tuple2.f0;
                        Integer userAge = tuple2.f1;
                        out.collect(Tuple6.of(userId, value.f1, value.f2, value.f3, username, userAge));
                    }
                }

            }

            // 处理广播流中更新的数据
            @Override
            public void processBroadcastElement(Map<String, Tuple2<String, Integer>> value, BroadcastProcessFunction<Tuple4<String, String, String, Integer>, Map<String, Tuple2<String, Integer>>, Tuple6<String, String, String, Integer, String, Integer>>.Context ctx, Collector<Tuple6<String, String, String, Integer, String, Integer>> out) throws Exception {
                // value是间隔一段时间从mysql中获取最新的规则

                // 先根据状态描述器获取历史广播的状态
                BroadcastState<Void, Map<String, Tuple2<String, Integer>>> broadcastState = ctx.getBroadcastState(descriptor);
                // 清空历史的规则
                broadcastState.clear();
                // 在将最新的规则放到广播中
                broadcastState.put(null, value);


            }
        });

        result.print();

        environment.execute("broadcast");
    }
}
