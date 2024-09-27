package broadcast;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.EnumTypeInfo;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BroadStateDemo {
    public enum Shape {
        RECTANGLE, TRIANGLE, CIRCLE
    }

    public enum Color {
        RED, GREEN, BLUE
    }
    private static class Item {

        private final Shape shape;
        private final Color color;

        Item(final Shape shape, final Color color) {
            this.color = color;
            this.shape = shape;
        }

        Shape getShape() {
            return shape;
        }

        public Color getColor() {
            return color;
        }

        @Override
        public String toString() {
            return "Item{" +
                    "shape=" + shape +
                    ", color=" + color +
                    '}';
        }
    }
    final static Class<Tuple2<Shape, Shape>> typedTuple = (Class<Tuple2<Shape, Shape>>) (Class<?>) Tuple2.class;

    final static TupleTypeInfo<Tuple2<Shape, Shape>> tupleTypeInfo = new TupleTypeInfo<>(typedTuple,
            new EnumTypeInfo<>(Shape.class),
            new EnumTypeInfo<>(Shape.class)
            );
    public static void main(String[] args) {

        //待广播的规则
        final List<Tuple2<Shape, Shape>> rules = new ArrayList<>();
        rules.add(new Tuple2<>(Shape.RECTANGLE, Shape.TRIANGLE));

        final List<Item> keyedInput = new ArrayList<>();
        keyedInput.add(new Item(Shape.RECTANGLE, Color.GREEN));
        keyedInput.add(new Item(Shape.TRIANGLE, Color.BLUE));
        keyedInput.add(new Item(Shape.TRIANGLE, Color.RED));
        keyedInput.add(new Item(Shape.CIRCLE, Color.BLUE));
        keyedInput.add(new Item(Shape.CIRCLE, Color.GREEN));
        keyedInput.add(new Item(Shape.TRIANGLE, Color.BLUE));
        keyedInput.add(new Item(Shape.RECTANGLE, Color.GREEN));
        keyedInput.add(new Item(Shape.CIRCLE, Color.GREEN));
        keyedInput.add(new Item(Shape.TRIANGLE, Color.GREEN));

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建广播状态
        MapStateDescriptor<String, Tuple2<Shape, Shape>> descriptor = new MapStateDescriptor<>("RulesBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,tupleTypeInfo);

        //数据源1
        KeyedStream<Item, Color> keyedStream = environment.fromCollection(keyedInput)
                .rebalance()
                .map(item -> item)
                .setParallelism(4)
                .keyBy(Item::getColor);

        //数据源2
        BroadcastStream<Tuple2<Shape, Shape>> broadcastRulesStream = environment.fromCollection(rules)
                .flatMap(new FlatMapFunction<Tuple2<Shape, Shape>, Tuple2<Shape, Shape>>() {
                    private static final long serialVersionUID = 6462244253439410814L;

                    @Override
                    public void flatMap(Tuple2<Shape, Shape> value, Collector<Tuple2<Shape, Shape>> out) {
                        out.collect(value);
                    }
                })
                .setParallelism(4)
                .broadcast(descriptor);

        keyedStream.connect(broadcastRulesStream).process(new MatchFunction());

    }
    public static class MatchFunction extends KeyedBroadcastProcessFunction<Color, Item, Tuple2<Shape, Shape>, String> {

        private int counter = 0;

        private final MapStateDescriptor<String, List<Item>> matchStateDesc =
                new MapStateDescriptor<>("items", BasicTypeInfo.STRING_TYPE_INFO, new ListTypeInfo<>(Item.class));

        private final MapStateDescriptor<String, Tuple2<Shape, Shape>> broadcastStateDescriptor =
                new MapStateDescriptor<>("RulesBroadcastState", BasicTypeInfo.STRING_TYPE_INFO, tupleTypeInfo);

        @Override
        public void processBroadcastElement(Tuple2<Shape, Shape> value, Context ctx, Collector<String> out) throws Exception {
            ctx.getBroadcastState(broadcastStateDescriptor).put("Rule_" + counter++, value);
            System.out.println("ADDED: Rule_" + (counter-1) + " " + value);
        }

        @Override
        public void processElement(Item nextItem, ReadOnlyContext ctx, Collector<String> out) throws Exception {

            final MapState<String, List<Item>> partialMatches = getRuntimeContext().getMapState(matchStateDesc);
            final Shape shapeOfNextItem = nextItem.getShape();

            System.out.println("SAW: " + nextItem);
            for (Map.Entry<String, Tuple2<Shape, Shape>> entry: ctx.getBroadcastState(broadcastStateDescriptor).immutableEntries()) {
                final String ruleName = entry.getKey();
                final Tuple2<Shape, Shape> rule = entry.getValue();

                List<Item> partialsForThisRule = partialMatches.get(ruleName);
                if (partialsForThisRule == null) {
                    partialsForThisRule = new ArrayList<>();
                }

                if (shapeOfNextItem == rule.f1 && !partialsForThisRule.isEmpty()) {
                    for (Item i : partialsForThisRule) {
                        out.collect("MATCH: " + i + " - " + nextItem);
                    }
                    partialsForThisRule.clear();
                }

                if (shapeOfNextItem == rule.f0) {
                    partialsForThisRule.add(nextItem);
                }

                if (partialsForThisRule.isEmpty()) {
                    partialMatches.remove(ruleName);
                } else {
                    partialMatches.put(ruleName, partialsForThisRule);
                }
            }
        }
    }
}
