package broadcast;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

/**
 * 读取mysql中的数据
 */
public class MysqlSource  extends RichSourceFunction<Map<String, Tuple2<String, Integer>>> {
    private boolean flag = true;
    private Connection conn = null;
    private PreparedStatement ps = null;
    private ResultSet rs = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        conn = DriverManager.getConnection("jdbc:mysql://172.16.48.49:3306/boradcast_info", "root", "Tbds@12345");
        String sql = "select `userID`, `userName`, `userAge` from `user_info`";
        ps = conn.prepareStatement(sql);
    }

    @Override
    public void run(SourceContext<Map<String, Tuple2<String, Integer>>> ctx) throws Exception {
        while (flag) {
            HashMap<String, Tuple2<String, Integer>> hashMap = new HashMap<>();

            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                String userID = rs.getString("userID");
                String userName = rs.getString("userName");
                int userAge = rs.getInt("userAge");
                hashMap.put(userID, Tuple2.of(userName, userAge));
            }

            ctx.collect(hashMap);
            Thread.sleep(5000); // 每隔5s更新一下用户的配置信息
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }

    @Override
    public void close() throws Exception {
        if (conn != null) conn.close();
        if (ps != null) ps.close();
        if (rs != null) rs.close();
    }
}
