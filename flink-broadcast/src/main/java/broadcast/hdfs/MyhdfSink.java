package broadcast.hdfs;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MyhdfSink extends RichSinkFunction<String> {
    private Logger logger = LoggerFactory.getLogger(MyhdfSink.class);
    private FileSystem fileSystem=null;
    private SimpleDateFormat sd = null;
    private String path = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        // hdfs path
        path = "hdfs://172.16.48.124:4007/tmp";
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        configuration.setBoolean("dfs.support.append", true);
        fileSystem = FileSystem.get(configuration);


    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        super.invoke(value, context);

        if (value != null) {
            String format = sd.format(new Date());
            StringBuilder builder = new StringBuilder();
            builder.append(path).append(format);
            Path path1 = new Path(builder.toString());
            logger.info("创建的文件路径: {}", path1);
            FSDataOutputStream outputStream = null;
            try {
                if (fileSystem.exists(path1)) {
                    outputStream = fileSystem.append(path1);
                }else  {
                    outputStream = fileSystem.create(path1, false);
                }
                outputStream.write((value + "\n").getBytes("UTF-8"));
            }catch (IOException e) {
                logger.error("写入数据报错: {}", e.getMessage());
            }finally {
                outputStream.close();
            }

        }

    }
    @Override
    public void close() throws Exception {
        super.close();
        fileSystem.close();
    }
}
