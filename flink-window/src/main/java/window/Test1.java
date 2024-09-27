package window;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Random;

/**
 * 测试函数
 */
public class Test1 {
    private static final Logger LOG = LoggerFactory.getLogger(Test1.class);
    public static void main(String[] args) {
        Random random = new Random();
        double temperature = random.nextGaussian() * 100;
//        System.out.println("温度值: "+ temperature);
        LOG.info(" 温度值");
        LOG.error("ee");
        LOG.debug("debug");
        LOG.warn("hdwfe");

        long currentTimeMillis = System.currentTimeMillis();
        Date date = new Date();
        long timestamp = date.getTime();
        LOG.info("当前系统的时间；{}", currentTimeMillis);
        LOG.info("当前系统的时间；{}", timestamp);

    }
}
