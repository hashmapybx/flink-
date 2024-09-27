package broadcast.nexmark;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkRunNexMark17 {
    private static Logger LOG = LoggerFactory.getLogger(FlinkRunNexMark17.class);
    public static void main(String[] args) throws Exception {

        if (args.length != 1) {
            LOG.info("====input generate data speed======");
            System.exit(-1);
        }
        int speed = Integer.parseInt(args[0]);
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(executionEnvironment);

        streamTableEnvironment.executeSql("CREATE TEMPORARY TABLE nexmark_table(  \n" +
                "    event_type INT,\n" +
                "    person ROW < id BIGINT, name VARCHAR, emailAddress VARCHAR, creditCard VARCHAR, city VARCHAR, state VARCHAR, dateTime TIMESTAMP(3), extra VARCHAR >,\n" +
                "    auction ROW < id BIGINT, itemName VARCHAR, description VARCHAR, initialBid BIGINT, reserve BIGINT, dateTime TIMESTAMP(3), expires TIMESTAMP(3), seller BIGINT, category BIGINT, extra VARCHAR >,\n" +
                "    bid ROW < auction BIGINT, bidder BIGINT, price BIGINT, channel VARCHAR, url VARCHAR, dateTime TIMESTAMP(3), extra VARCHAR >,\n" +
                "    dateTime AS CASE \n" +
                "        WHEN event_type = 0 \n" +
                "        THEN person.dateTime \n" +
                "        WHEN event_type = 1 \n" +
                "        THEN auction.dateTime \n" +
                "        ELSE bid.dateTime \n" +
                "    END,\n" +
                "    WATERMARK FOR dateTime AS dateTime - INTERVAL '4' SECOND\n" +
                ") \n" +
                "WITH (\n" +
                "    'connector' = 'nexmark',\n" +
                "    'first-event.rate' = '" + speed + "',\n" +
                "    'next-event.rate' = '" + speed + "',\n" +
                "    'events.num' = '100000000',\n" +
                "    'person.proportion' = '2',\n" +
                "    'auction.proportion' = '6',\n" +
                "    'bid.proportion' = '92'\n" +
                "); \n");
        streamTableEnvironment.executeSql("CREATE TEMPORARY TABLE discard_sink (\n" +
                "    auction BIGINT,\n" +
                "    `day` VARCHAR,\n" +
                "    total_bids BIGINT,\n" +
                "    rank1_bids BIGINT,\n" +
                "    rank2_bids BIGINT,\n" +
                "    rank3_bids BIGINT,\n" +
                "    min_price BIGINT,\n" +
                "    max_price BIGINT,\n" +
                "    avg_price BIGINT,\n" +
                "    sum_price BIGINT\n" +
                ") \n" +
                "WITH ('connector' = 'blackhole');");
        streamTableEnvironment.executeSql("CREATE TEMPORARY VIEW bid AS\n" +
                "    SELECT \n" +
                "        bid.auction,\n" +
                "        bid.bidder,\n" +
                "        bid.price,\n" +
                "        bid.channel,\n" +
                "        bid.url,\n" +
                "        dateTime,\n" +
                "        bid.extra \n" +
                "    FROM\n" +
                "        nexmark_table \n" +
                "    WHERE\n" +
                "        event_type = 2;");
        streamTableEnvironment.executeSql("INSERT INTO discard_sink\n" +
                "    SELECT \n" +
                "        auction,\n" +
                "        DATE_FORMAT(dateTime, 'yyyy-MM-dd') as `day`,\n" +
                "        count(*) AS total_bids,\n" +
                "        count(*) \n" +
                "            filter(where price < 10000) AS rank1_bids,\n" +
                "        count(*) \n" +
                "            filter(where price >= 10000 \n" +
                "                and price < 1000000) AS rank2_bids,\n" +
                "        count(*) \n" +
                "            filter(where price >= 1000000) AS rank3_bids,\n" +
                "        min(price) AS min_price,\n" +
                "        max(price) AS max_price,\n" +
                "        avg(price) AS avg_price,\n" +
                "        sum(price) AS sum_price \n" +
                "    FROM\n" +
                "        bid \n" +
                "    GROUP BY\n" +
                "        auction, DATE_FORMAT(dateTime, 'yyyy-MM-dd');");

//        executionEnvironment.execute("write query17");

    }
}
