import lombok.Data;

import java.io.Serializable;

/*
*@ClassName:task.Message
 @Description:TODO
 @Author:
 @Date:2018/12/3 15:08 
 @Version:v1.0
*/
@Data
public class Message implements Serializable {
    private String value;
    private String id;
    private String compType;
    private String configType;
    private String cluster;
    private String compName;
    private String hostIp;
    private String userIp;
    private String metricCode;
    private String metricType;
    private String metricValue;
    private String collectTime;
    private String seqId;
}
