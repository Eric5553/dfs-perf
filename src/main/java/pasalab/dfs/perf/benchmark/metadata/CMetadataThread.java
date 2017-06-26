package pasalab.dfs.perf.benchmark.metadata;

import pasalab.dfs.perf.basic.PerfThread;
import pasalab.dfs.perf.basic.TaskConfiguration;
import pasalab.dfs.perf.conf.PerfConf;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class CMetadataThread extends PerfThread {

    private boolean mSuccess;

    private String mHostname;
    private int mPort;
    private int mClientsNum;
    private long mOpTimeMs;
    private String mWorkDir;

    public boolean getSuccess() {
        return mSuccess;
    }

    public boolean setupThread(TaskConfiguration taskConf) {
        String dfsAddress = PerfConf.get().DFS_ADDRESS;
        // 10 = "alluxio://"
        mHostname = dfsAddress.substring(10, dfsAddress.lastIndexOf(':'));
        mPort = Integer.valueOf(dfsAddress.substring(dfsAddress.lastIndexOf(':') + 1));
        mClientsNum = taskConf.getIntProperty("clients.per.thread");
        mOpTimeMs = taskConf.getIntProperty("op.second.per.thread") * 1000;
        mWorkDir = taskConf.getProperty("work.dir");

        return true;
    }

    public boolean cleanupThread(TaskConfiguration taskConf) {
        return true;
    }

    public void run() {
        // form the command
        String[] cmd = new String[]{"sh", "-c", getShellCommand()};

        try {
            // start the process and consume the stdout/stderr
            Process p = Runtime.getRuntime().exec(cmd);
            communicateWithProcess(p);

            // get job return status
            p.waitFor();
            LOG.info("shell return value is " + p.exitValue());
            mSuccess = p.exitValue() == 0 ? true : false;
        } catch (IOException e) {
            LOG.error(e.getStackTrace());
            mSuccess = false;
        } catch (InterruptedException e) {
            LOG.error(e.getStackTrace());
            mSuccess = false;
        }
    }

    private String getShellCommand() {
        StringBuffer sb = new StringBuffer();

        sb.append(PerfConf.get().LIBALLUXIO2_HOME +"/test/dfs-perf-performance/" + mTestCase + ".sh");
        sb.append(" " + mWorkDir);
        sb.append(" " + mClientsNum);
        sb.append(" " + mOpTimeMs);
        sb.append(" " + mHostname);
        sb.append(" " + mPort);
        sb.append(" " + mTaskId);
        sb.append(" " + PerfConf.get().DFS_PERF_HOME);
        sb.append(" " + mNodeName);
        sb.append(" " + PerfConf.get().LIBALLUXIO2_HOME);
        sb.append(" " + mId);

        LOG.info(sb.toString());
        return sb.toString();
    }

    private void communicateWithProcess(final Process p) {
        new Thread() {
            @Override public void run() {
                String line;
                final BufferedReader stdOut = new BufferedReader(new InputStreamReader(p.getInputStream()));

                try {
                    while ((line = stdOut.readLine()) != null) {
                        LOG.info(line);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }.start();

        new Thread() {
            @Override public void run() {
                String line;
                final BufferedReader stdErr = new BufferedReader(new InputStreamReader(p.getErrorStream()));

                try {
                    while ((line = stdErr.readLine()) != null) {
                        LOG.error(line);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }.start();
    }
}
