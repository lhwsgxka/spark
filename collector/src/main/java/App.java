import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.SystemConfiguration;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.io.InputStream;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

//项目入口类
public class App {

    public static void main(String[] args) throws DocumentException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        ScheduledExecutorService pool = Executors.newScheduledThreadPool(15);
        //加载配置文件
        SAXReader reader = new SAXReader();
        InputStream inputStream = App.class.getClassLoader().getResourceAsStream("task.xml");
        //表示是task.xml
        Document document = reader.read(inputStream);
        Element root = document.getRootElement();

        List<Element> childElements = root.elements();
        for (Element child : childElements) {
            String period = child.element("period").getTextTrim();
            String className = child.element("class").getTextTrim();
            String type = child.element("type").getTextTrim();
            String name = child.element("name").getTextTrim();
            //通过反射机制   实例化对象
            MonitorTask instance = (MonitorTask) Class.forName(className).newInstance();
            //传入configuration,初始化
            Configuration conf = new SystemConfiguration();
            conf.setProperty("period", Long.valueOf(period));
            conf.setProperty("type", type);
            conf.setProperty("name", name);
            instance.init(conf);
            //启动任务
            pool.scheduleAtFixedRate(instance, 0, Long.valueOf(period), TimeUnit.SECONDS);
        }


    }
}
