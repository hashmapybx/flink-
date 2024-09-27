package broadcast;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class BoradCastStateDemo2 {
    private static  final Logger logger = LoggerFactory.getLogger(BoradCastStateDemo2.class);
    public static void main(String[] args) throws Exception {

        //创建规则
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.setParallelism(2);

        //数据源
        DataStreamSource<String> source = environment.addSource(new RichSourceFunction<String>() {
            private boolean isRunning = true;
            String[] data = new String[] {"java", "python", "scala"};

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                int size = data.length;
                while (isRunning) {
                    TimeUnit.SECONDS.sleep(3);
                    int seed = (int) (Math.random() * size);
                    ctx.collect(data[seed]);
                    logger.info("第一个流发送：{}", data[seed]);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });

//        source.print();
        //定义广播规则 去从数据源中去获取数据
        MapStateDescriptor<String, String> configFilter = new MapStateDescriptor<>("configFilter", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

        //进行广播操作
        BroadcastStream<String> broadcastConfig = source.setParallelism(1).broadcast(configFilter);

        //模拟第二个数据源
        DataStreamSource<String> streamSource = environment.addSource(new RichSourceFunction<String>() {
            private boolean isRunning = true;
            String[] data = new String[]{"java代码量太大了", "python代码，好学"};

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                int size = data.length;
                while (isRunning) {
                    TimeUnit.SECONDS.sleep(2);
                    int seed = (int) (Math.random() * size);
                    ctx.collect(data[seed]);
                    logger.info("第二个流发送的数据: {}", data[seed]);
                }

            }

            @Override
            public void cancel() {
                isRunning = true;
            }
        });

        //对于广播的数据进行关联
        SingleOutputStreamOperator<String> resultStream = streamSource.connect(broadcastConfig).process(new BroadcastProcessFunction<String, String, String>() {

            //设置拦截的关键字
            private String keyWords = null;
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                keyWords = "java";
                logger.info("初始化的关键字: {}" , keyWords);
            }

            //处理每一条数据
            @Override
            public void processElement(String value, BroadcastProcessFunction<String, String, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
                if (value.contains(keyWords)) {
                        out.collect("拦截消息:" + value + ", 是因为匹配到规则:" + keyWords);
                }
            }

            //对于广播变量进行更新
            @Override
            public void processBroadcastElement(String value, BroadcastProcessFunction<String, String, String>.Context ctx, Collector<String> out) throws Exception {
                keyWords = value;
                logger.info("更新关键字: {}" , value);
            }
        });
        resultStream.print("result");
        environment.execute();
    }
}
