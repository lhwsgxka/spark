package task.hdfs;

import org.apache.commons.configuration.Configuration;

public interface MonitorTask extends Runnable {
    void init(Configuration conf);

    void clean();
}
