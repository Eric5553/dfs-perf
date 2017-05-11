package pasalab.dfs.perf.benchmark.simpleread;

import pasalab.dfs.perf.basic.PerfThread;
import pasalab.dfs.perf.benchmark.SimpleTaskContext;

import java.io.File;
import java.io.IOException;

public class CSimpleReadTaskContext extends SimpleTaskContext {
    @Override
    public void setFromThread(PerfThread[] threads) {
    }

    @Override
    public void writeToFile(File file) throws IOException {
    }
}
