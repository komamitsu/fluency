package org.komamitsu.fluency.sender;

import java.io.Closeable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class Sender
    implements Closeable
{
    protected final Config config;

    protected Sender(Config config)
    {
        this.config = config;
    }

    abstract protected Config getConfig();

    public synchronized void send(ByteBuffer data)
            throws IOException
    {
        sendInternalWithRestoreBufferPositions(Arrays.asList(data), null);
    }

    public synchronized void send(List<ByteBuffer> dataList)
            throws IOException
    {
        sendInternalWithRestoreBufferPositions(dataList, null);
    }

    public void sendWithAck(List<ByteBuffer> dataList, byte[] ackToken)
            throws IOException
    {
        sendInternalWithRestoreBufferPositions(dataList, ackToken);
    }

    private void sendInternalWithRestoreBufferPositions(List<ByteBuffer> dataList, byte[] ackToken)
            throws IOException
    {
        List<Integer> positions = new ArrayList<Integer>(dataList.size());
        for (ByteBuffer data : dataList) {
            positions.add(data.position());
        }

        try {
            sendInternal(dataList, ackToken);
        }
        catch (Exception e) {
            for (int i = 0; i < dataList.size(); i++) {
                dataList.get(i).position(positions.get(i));
            }
            if (e instanceof IOException) {
                throw (IOException)e;
            }
            else {
                throw new IOException(e);
            }
        }
    }

    @Override
    public String toString()
    {
        return "Sender{" +
                "config=" + config +
                '}';
    }

    abstract protected void sendInternal(List<ByteBuffer> dataList, byte[] ackToken) throws IOException;

    public abstract static class Config<SenderImpl extends Sender, SenderConfigImpl extends Config<SenderImpl, SenderConfigImpl>>
    {
        protected abstract SenderConfigImpl self();
        public abstract SenderImpl createInstance();
    }
}
