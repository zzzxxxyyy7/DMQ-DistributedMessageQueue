import com.ycatch.utils.MMapUtil;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TestMMapUtil {

    private MMapUtil mMapUtil;
    private static final String filePath = "/Users/zhongyujie/Desktop/项目/Java项目/DistributedMessageQueue/broker/store/order_cancel_topic/00000000";

    @Before
    public void setUp() throws IOException {
        mMapUtil = new MMapUtil();
        mMapUtil.loadFileInMMap(filePath,
            0, 1024 * 1024);
    }

    @Test
    public void testLoadFile() throws IOException {
        mMapUtil.loadFileInMMap(filePath,
            0, 1024 * 1024);
    }

    @Test
    public void testWriteAndReadContent() {
        byte[] content = "hello world".getBytes();
        mMapUtil.writeContent(content);
        byte[] bytes = mMapUtil.readContent(0, content.length);
        System.out.println(new String(bytes));
        mMapUtil.clear();
    }

}
