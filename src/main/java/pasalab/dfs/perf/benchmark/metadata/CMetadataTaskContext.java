package pasalab.dfs.perf.benchmark.metadata;

import pasalab.dfs.perf.basic.PerfThread;
import pasalab.dfs.perf.benchmark.SimpleTaskContext;

import java.io.File;
import java.io.IOException;

public class CMetadataTaskContext extends SimpleTaskContext {
    public void setFromThread(PerfThread[] threads) {
    }

    @Override
    public void writeToFile(File file) throws IOException {
    }
}
