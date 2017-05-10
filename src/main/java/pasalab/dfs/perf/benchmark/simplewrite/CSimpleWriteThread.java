package pasalab.dfs.perf.benchmark.simplewrite;

import pasalab.dfs.perf.basic.PerfThread;
import pasalab.dfs.perf.basic.TaskConfiguration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class CSimpleWriteThread extends PerfThread {

    private boolean mSuccess;

    private String mWriteDir;
    private int mBufferSize;
    private long mFileLength;
    private int mFilesNum;
    private long mBlockSize;

    public void run() {
        // form the command
        String[] cmd = new String[] {"sh", "-c", getShellCommand()};

        try {
            // start the process and consume the stdout/stderr
            Process p = Runtime.getRuntime().exec(cmd);
            communicateWithProcess(p);

            // get job return status
            int rc = p.waitFor();
            mSuccess = rc == 0 ? true : false;
        } catch (IOException e) {
            LOG.error(e.getStackTrace());
            mSuccess = false;
        } catch (InterruptedException e) {
            LOG.error(e.getStackTrace());
            mSuccess = false;
        }
    }

    @Override
    public boolean setupThread(TaskConfiguration taskConf) {
        mBufferSize = taskConf.getIntProperty("buffer.size.bytes");
        mFileLength = taskConf.getLongProperty("file.length.bytes");
        mFilesNum = taskConf.getIntProperty("files.per.thread");
        mBlockSize = taskConf.getLongProperty("block.size.bytes");
        mWriteDir = taskConf.getProperty("write.dir");

        mSuccess = false;
        return true;
    }

    @Override
    public boolean cleanupThread(TaskConfiguration taskConf) {
        return true;
    }

    private String getShellCommand() {
        StringBuffer sb = new StringBuffer();

        sb.append("/liballuxio2/test/performance/" + mTestCase + ".sh");
        sb.append(" " + mWriteDir);
        sb.append(" " + mBufferSize);
        sb.append(" " + mFileLength);
        sb.append(" " + mFilesNum);
        sb.append(" " + mBlockSize);

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

    public boolean getSuccess() {
        return mSuccess;
    }
}
