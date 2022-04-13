package watermark;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.concurrent.TimeUnit;

public class SourceFromFile extends RichSourceFunction<String> {
    private volatile Boolean isRunning = true;

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        BufferedReader bufferedReader = new BufferedReader(new FileReader("C:\\Users\\keguang\\Desktop\\simple.etl.csv"));
        while (isRunning) {
            String line = bufferedReader.readLine();
            if (StringUtils.isBlank(line)) {
                continue;
            }
            ctx.collect(line);
            TimeUnit.SECONDS.sleep(10);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
