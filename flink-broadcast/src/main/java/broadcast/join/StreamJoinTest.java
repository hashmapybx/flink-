package broadcast.join;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.StreamEnv;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Random;

public class StreamJoinTest {
    private static Logger LOG = LoggerFactory.getLogger(StreamJoinTest.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamEnv.getInstance();

        environment.setParallelism(1);
        //设置checkpoint配置
        environment.enableCheckpointing(30000);
        environment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        environment.getCheckpointConfig().setMinPauseBetweenCheckpoints(500); //设置ck之间的间隔时间
        environment.getCheckpointConfig().setCheckpointTimeout(60000); //ck的超市时间
        environment.getCheckpointConfig().setMaxConcurrentCheckpoints(1); //同一时间只是允许一个ck进行
        //在job 中止后继续保留ck
        environment.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //构建商品维表流
        DataStreamSource<YGoods> yGoodsDataStreamSource = environment.addSource(new RichSourceFunction<YGoods>() {
            private boolean isRunning;

            @Override
            public void open(Configuration parameters) throws Exception {
                isRunning = true;
            }

            @Override
            public void run(SourceContext<YGoods> sourceContext) throws Exception {
                Random random = new Random();

                while (isRunning) {
                    String productId = "P_" + random.nextInt(100);
                    String productName = "name_" + random.nextInt(10);
                    BigDecimal price = BigDecimal.valueOf(random.nextDouble());
                    price.setScale(2, RoundingMode.HALF_UP);
                    sourceContext.collect(new YGoods(productId, productName, price));
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });

        //构建订单流
        DataStreamSource<YOrders> yOrdersDataStreamSource = environment.addSource(new RichSourceFunction<YOrders>() {
            private boolean isRunning;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                isRunning = true;
            }

            @Override
            public void run(SourceContext<YOrders> sourceContext) throws Exception {
                Random random = new Random();

                while (isRunning) {
                    String productId = "P_" + random.nextInt(100);
                    String itemId = "item_" + (random.nextInt(10) + 100);
                    int count = random.nextInt(1000);
                    sourceContext.collect(new YOrders(itemId, productId, count));
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });
        // 给数据流添加时间
        SingleOutputStreamOperator<YGoods> yGoodsSingleOutputStreamOperator = yGoodsDataStreamSource
                .assignTimestampsAndWatermarks(new WatermarkStrategy<YGoods>() {
            @Override
            public WatermarkGenerator<YGoods> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new WatermarkGenerator<YGoods>() {

                    @Override
                    public void onEvent(YGoods event, long eventTimestamp, WatermarkOutput output) {
                        output.emitWatermark(new Watermark(System.currentTimeMillis()));
                    }

                    @Override
                    public void onPeriodicEmit(WatermarkOutput output) {
                        output.emitWatermark(new Watermark(System.currentTimeMillis()));
                    }
                };
            }

                    @Override
                    public TimestampAssigner<YGoods> createTimestampAssigner(TimestampAssignerSupplier.Context context) {

                        return new TimestampAssigner<YGoods>(){
                            @Override
                            public long extractTimestamp(YGoods element, long recordTimestamp) {
                                return System.currentTimeMillis();
                            }
                        };

                    }
                });

        SingleOutputStreamOperator<YOrders> yOrdersSingleOutputStreamOperator = yOrdersDataStreamSource.assignTimestampsAndWatermarks(new WatermarkStrategy<YOrders>() {
            @Override
            public WatermarkGenerator<YOrders> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new WatermarkGenerator<YOrders>() {
                    @Override
                    public void onEvent(YOrders event, long eventTimestamp, WatermarkOutput output) {
                        output.emitWatermark(new Watermark(System.currentTimeMillis()));
                    }

                    @Override
                    public void onPeriodicEmit(WatermarkOutput output) {
                        output.emitWatermark(new Watermark(System.currentTimeMillis()));
                    }
                };
            }

            @Override
            public TimestampAssigner<YOrders> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                return new TimestampAssigner<YOrders>() {
                    @Override
                    public long extractTimestamp(YOrders element, long recordTimestamp) {
                        return System.currentTimeMillis();
                    }
                };
            }
        });

        DataStream<YOrderDetails> joinResultDS = yOrdersDataStreamSource.join(yGoodsSingleOutputStreamOperator)
                .where(new KeySelector<YOrders, String>() {
                    @Override
                    public String getKey(YOrders value) throws Exception {
                        return value.getGoodsId();
                    }
                })
                .equalTo(
                        new KeySelector<YGoods, String>() {
                            @Override
                            public String getKey(YGoods value) throws Exception {
                                return value.getGoodsId();
                            }
                        }
                ).window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .apply(new JoinFunction<YOrders, YGoods, YOrderDetails>() {

                    @Override
                    public YOrderDetails join(YOrders first, YGoods second) throws Exception {
                        LOG.info("执行join: order-> {}, good-> {}", first, second);
                        return new YOrderDetails(second.getGoodsId(), second.getGoodsName(), first.getCount(), second.getGoodsPrice().multiply(BigDecimal.valueOf(first.getCount())));
                    }
                });
        joinResultDS.map(new MapFunction<YOrderDetails, String>() {
            @Override
            public String map(YOrderDetails value) throws Exception {
                LOG.info("join result: {}", value.toString());
                return value.toString();
            }
        }).name("map");
        joinResultDS.print().name("print");
        environment.execute("join");

    }
}
