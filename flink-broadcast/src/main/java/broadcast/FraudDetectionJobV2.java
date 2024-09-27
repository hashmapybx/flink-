package broadcast;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import pojo.Alert;
import pojo.Transaction;
import pojo.TransactionSource;


/**
 * 官网的信用卡欺诈检测
 * 检测规则：对于一个账户，如果出现小于 $1 美元的交易后紧跟着一个大于 $500 的交易，就输出一个报警信息
 * 在这个场景中，要考虑到前一次是小于1，而中间间隔了几次那这个不算报警
 * 如果前一笔是小于1是A账户的，后面1笔是B账户的大于500这个也不需要告警
 *
 * 当两笔交易的间隔时间大于1min以上则是正常的，不需要报警 在FraudDetectionJobV2中在去更新规则
 *
 *
 */
public class FraudDetectionJobV2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Transaction> streamOperator = environment.addSource(new TransactionSource()).name("tran");

        SingleOutputStreamOperator<Alert> alters = streamOperator.keyBy(Transaction::getAccountId).process(new FraudDetector3()).name("fraud-detect");

        alters.addSink(new AlertSink()).name("send alert");

        environment.execute("Fraud Detection");
    }

}
