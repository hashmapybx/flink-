package broadcast;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import pojo.Transaction;
import pojo.TransactionSource;

import java.util.Map;

public class BroadcastDemo3 {

    //广播流的状态
    private static MapStateDescriptor<String, String> ruleStateDescriptor = new MapStateDescriptor<>(
            "RulesBroadcastState",
            BasicTypeInfo.STRING_TYPE_INFO,
            TypeInformation.of(new TypeHint<String>() {}));

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        //业务流的数据从交易模拟器中输入
        DataStream<Transaction> dataStream = env
                .addSource(new TransactionSource()) //TransactionSource可以查看前面章节，有源码分析讲解
                .name("transactions");
        KeyedStream<Transaction, Long> keyStream = dataStream.keyBy(item -> item.getAccountId());

        //广播流的规则，从Socket输入
        DataStream<String> ruleStream = env.socketTextStream("localhost", 7777);
        BroadcastStream<String> ruleBroadcastStream = ruleStream.broadcast(ruleStateDescriptor);

        //connect() 方法需要由非广播流来进行调用，BroadcastStream 作为参数传入
        keyStream.connect(ruleBroadcastStream).process(new State_7_BroadcastState_Inner());

        env.execute();
    }

    public static class State_7_BroadcastState_Inner extends KeyedBroadcastProcessFunction<Long, Transaction, String, Void> {

        @Override
        public void processElement(Transaction value, KeyedBroadcastProcessFunction<Long, Transaction, String, Void>.ReadOnlyContext ctx, Collector<Void> out) throws Exception {
            // 这里是处理非广播流的数据
            //模拟基于用户交易匹配匹配广播的规则，然后进行计算
            StringBuilder stringBuilder= new StringBuilder("");

            Iterable<Map.Entry<String, String>> entries = ctx.getBroadcastState(ruleStateDescriptor).immutableEntries();

            for (Map.Entry<String,String> entry: entries
                 ) {
                // 读取广播规则
                final String ruleName = entry.getKey();
                final String rule = entry.getValue();

                stringBuilder.append(ruleName).append(":").append(rule).append(";");
            }
            System.out.println("当前用户:"+value.getAccountId()+" 匹配到的规则:" + stringBuilder.toString());
        }

        @Override
        public void processBroadcastElement(String value, KeyedBroadcastProcessFunction<Long, Transaction, String, Void>.Context ctx, Collector<Void> out) throws Exception {
            //处理广播流中的元素
            //当广播流中有规则了，将规则放入state
            ctx.getBroadcastState(ruleStateDescriptor).put(value, value);
        }
    }

    // https://www.itzhimei.com/archives/2367.html



    /*
Socket一次输入：
rule1
rule2
rule3
观察日志输出。

当Socket没有输入规则时，控制台的输出：
当前用户:1 匹配到的规则:
当前用户:2 匹配到的规则:
当前用户:3 匹配到的规则:
当前用户:4 匹配到的规则:
当前用户:5 匹配到的规则:
当前用户:1 匹配到的规则:
当前用户:2 匹配到的规则:
当前用户:3 匹配到的规则:
当前用户:4 匹配到的规则:
===========================================================
当Socket输入rule1规则时，控制台的输出：
当前用户:2 匹配到的规则:rule1:rule1;
当前用户:3 匹配到的规则:rule1:rule1;
当前用户:4 匹配到的规则:rule1:rule1;
当前用户:5 匹配到的规则:rule1:rule1;
当前用户:1 匹配到的规则:rule1:rule1;
当前用户:2 匹配到的规则:rule1:rule1;
当前用户:3 匹配到的规则:rule1:rule1;
当前用户:4 匹配到的规则:rule1:rule1;
当前用户:5 匹配到的规则:rule1:rule1;
当前用户:1 匹配到的规则:rule1:rule1;
当前用户:2 匹配到的规则:rule1:rule1;
当前用户:3 匹配到的规则:rule1:rule1;
===========================================================
当Socket输入rule2规则时，控制台的输出：
当前用户:5 匹配到的规则:rule1:rule1;rule2:rule2;
当前用户:1 匹配到的规则:rule1:rule1;rule2:rule2;
当前用户:2 匹配到的规则:rule1:rule1;rule2:rule2;
当前用户:3 匹配到的规则:rule1:rule1;rule2:rule2;
当前用户:4 匹配到的规则:rule1:rule1;rule2:rule2;
当前用户:5 匹配到的规则:rule1:rule1;rule2:rule2;
当前用户:1 匹配到的规则:rule1:rule1;rule2:rule2;
当前用户:2 匹配到的规则:rule1:rule1;rule2:rule2;
当前用户:3 匹配到的规则:rule1:rule1;rule2:rule2;
当前用户:4 匹配到的规则:rule1:rule1;rule2:rule2;
当前用户:5 匹配到的规则:rule1:rule1;rule2:rule2;
当前用户:1 匹配到的规则:rule1:rule1;rule2:rule2;

*/
}
