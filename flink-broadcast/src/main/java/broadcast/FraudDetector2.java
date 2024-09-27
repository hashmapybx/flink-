package broadcast;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import pojo.Alert;
import pojo.Transaction;

import java.util.Objects;

public class FraudDetector2 extends KeyedProcessFunction<Long, Transaction, Alert> {

    private static final long serialVersionUID = 1L;

    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long ONE_MINUTE = 60 * 1000;

    //前一次交易小于$1的状态标识 需要将前一次的状态保存
    private transient ValueState<Boolean> flagState;


    @Override
    public void open(Configuration parameters) throws Exception {
        //先创建状态的描述对象
        ValueStateDescriptor<Boolean> stateDescriptor = new ValueStateDescriptor<>("flag", Types.BOOLEAN);
        flagState = getRuntimeContext().getState(stateDescriptor);
    }

    @Override
    public void processElement(Transaction value, KeyedProcessFunction<Long, Transaction, Alert>.Context ctx, Collector<Alert> out) throws Exception {

       // 先要获取上一次计算的结果
        Boolean prefixOnceValue = flagState.value();

        if (Objects.nonNull(prefixOnceValue)) {
            // 上一次的状态非空，表示前面一笔交易是小于1的

            // 在判断当前的交易是不是大于500
            if (value.getAmount() > LARGE_AMOUNT) {
                // 则是要进行报警
                Alert alert = new Alert();
                alert.setId(value.getAccountId());
                out.collect(alert);
            }
            flagState.clear(); //清空状态
        }

        //判断当前交易是不是小于1，小于1则要更新状态
        if (value.getAmount() < SMALL_AMOUNT) {
            flagState.update(true);
        }
    }
}
