package pasalab.dfs.perf.benchmark.simpleread;

import pasalab.dfs.perf.basic.PerfThread;
import pasalab.dfs.perf.basic.TaskConfiguration;
import pasalab.dfs.perf.conf.PerfConf;
import pasalab.dfs.perf.fs.PerfFileSystem;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class CSimpleReadThread extends PerfThread {

    private boolean mSuccess;

    private String mHostname;
    private int mPort;
    private int mBufferSize;
    private int mFilesNum;
    private String mReadDir;
    private boolean mIsRandom;

    public boolean getSuccess() {
        return mSuccess;
    }

    public boolean setupThread(TaskConfiguration taskConf) {
        String dfsAddress = PerfConf.get().DFS_ADDRESS;

        if (!PerfFileSystem.isAlluxio(dfsAddress))
        {
            LOG.error("dfs is not alluxio, please modify the setting of DFS_PERF_DFS_ADDRESS.");
            mSuccess = false;
            return false;
        }

        // 10 = "alluxio://"
        mHostname = dfsAddress.substring(10, dfsAddress.lastIndexOf(':'));
        mPort = Integer.valueOf(dfsAddress.substring(dfsAddress.lastIndexOf(':') + 1));
        mBufferSize = taskConf.getIntProperty("buffer.size.bytes");
        mFilesNum = taskConf.getIntProperty("files.per.thread");
        mReadDir = taskConf.getProperty("read.dir");
        mIsRandom = "RANDOM".equals(taskConf.getProperty("read.mode"));

        return true;
    }

    public boolean cleanupThread(TaskConfiguration taskConf) {
        return true;
    }

    private String getShellCommand() {
        StringBuffer sb = new StringBuffer();

        sb.append(PerfConf.get().LIBALLUXIO2_HOME +"/test/performance/" + mTestCase + ".sh");
        sb.append(" " + mReadDir);
        sb.append(" " + mBufferSize);
        sb.append(" " + mFilesNum);
        sb.append(" " + (mIsRandom ? "1":"0"));
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

    public void run() {
        // form the command
        String[] cmd = new String[] {"sh", "-c", getShellCommand()};

        try {
            // start the process and consume the stdout/stderr
            Process p = Runtime.getRuntime().exec(cmd);
            communicateWithProcess(p);

            // get job return status
            p.waitFor();
            LOG.info("shell return value is "+p.exitValue());
            mSuccess = p.exitValue() == 0 ? true : false;
        } catch (IOException e) {
            LOG.error(e.getStackTrace());
            mSuccess = false;
        } catch (InterruptedException e) {
            LOG.error(e.getStackTrace());
            mSuccess = false;
        }
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
