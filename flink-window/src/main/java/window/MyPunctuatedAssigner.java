package window;

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.jetbrains.annotations.Nullable;
import watermark.WaterMark1;

/**
 *  间断式的生产watermark和周期型是不一样的
 *
 */
public class MyPunctuatedAssigner implements AssignerWithPunctuatedWatermarks<WaterMark1.SensorReading> {

    private Long bound = 60 * 1000L; // 延迟1分钟

    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(WaterMark1.SensorReading lastElement, long extractedTimestamp) {
        if (lastElement.getId().equals("远洋-1")) {
            return new Watermark(extractedTimestamp - bound);
        } else {
            return null;
        }
    }

    @Override
    public long extractTimestamp(WaterMark1.SensorReading element, long recordTimestamp) {
        return element.getVal1();
    }
}
