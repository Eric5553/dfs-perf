package pasalab.dfs.perf.benchmark.simpleread;

import pasalab.dfs.perf.basic.PerfTaskContext;
import pasalab.dfs.perf.benchmark.SimpleTask;
import pasalab.dfs.perf.conf.PerfConf;

public class CSimpleReadTask extends SimpleTask {
    @Override
    protected boolean setupTask(PerfTaskContext taskContext) {
        String writeDir = PerfConf.get().DFS_DIR + "/simple-read-write/" + mId;
        mTaskConf.addProperty("read.dir", writeDir);
        LOG.info("Write dir " + writeDir);
        return true;
    }
}
