import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.SystemDefaultHttpClient;
import task.hdfs.AbstractMonitorTask;
import util.KafkaUtil;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;

@Slf4j
public class MonitorTask extends AbstractMonitorTask {

    public void run() {
        String url = "http://192.168.203.37:50070/jmx?qry=java.lang:type=OperatingSystem";
        HttpGet httpGet = new HttpGet(url);
        try {
            CloseableHttpResponse response = new SystemDefaultHttpClient().execute(httpGet);
            InputStream content = response.getEntity().getContent();

            String jsonstr = IOUtils.toString(content);

            log.info("AvailableProcessors:{}", jsonstr);
            JsonObject jsonObject = new JsonParser().parse(jsonstr).getAsJsonObject();
            String availableProcessors = jsonObject.getAsJsonArray("beans")
                    .get(0).getAsJsonObject().get("AvailableProcessors")
                    .getAsString();
            log.info("AvailableProcessors:{}", availableProcessors);

            Message message = new Message();
            message.setCollectTime(new Date().toLocaleString());
            message.setHostIp("192.168.203.37");
            message.setMetricValue(availableProcessors + "");
            message.setCluster("hdfs");
            message.setCompType("hdfs");
            message.setConfigType("namenode");
            message.setMetricCode("availableProcessors");

            log.info("message:{}", new Gson().toJson(message));

            //发送出去
            KafkaUtil.send(new Gson().toJson(message), "666");
        } catch (IOException e) {

            log.error("error", e);
        }


    }

    public void clean() {

    }
}
