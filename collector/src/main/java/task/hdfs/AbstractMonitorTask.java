package task.hdfs;

import lombok.Data;
import org.apache.commons.configuration.Configuration;
@Data
public abstract class AbstractMonitorTask implements MonitorTask{
    private String name;
    private String type;
    private long period;

    public void init(Configuration conf) {
        setName(conf.getString("name"));
        setType(conf.getString("type"));
        setPeriod(conf.getLong("period"));
    }
}
