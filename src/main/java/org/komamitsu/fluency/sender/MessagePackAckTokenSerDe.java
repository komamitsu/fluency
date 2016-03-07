package org.komamitsu.fluency.sender;

import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.core.buffer.ArrayBufferInput;
import org.msgpack.core.buffer.MessageBufferInput;
import org.msgpack.core.buffer.MessageBufferOutput;
import org.msgpack.core.buffer.OutputStreamBufferOutput;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class MessagePackAckTokenSerDe
    implements AckTokenSerDe
{
    private final MessagePacker packer;
    private final MessageUnpacker unpacker;
    private final ByteArrayOutputStream outputStream = new ByteArrayOutputStream(64);

    public MessagePackAckTokenSerDe() {
        MessageBufferOutput bufferOutput = new OutputStreamBufferOutput(new ByteArrayOutputStream(0));
        this.packer = MessagePack.newDefaultPacker(bufferOutput);

        MessageBufferInput bufferInput = new ArrayBufferInput(new byte[0]);
        this.unpacker = MessagePack.newDefaultUnpacker(bufferInput);
    }

    @Override
    public byte[] pack(byte[] token)
            throws IOException
    {
        synchronized (packer) {
            outputStream.reset();
            MessageBufferOutput bufferOutput = new OutputStreamBufferOutput(outputStream);
            packer.reset(bufferOutput);
            packer.packMapHeader(1);
            packer.packString("chunk");
            packer.packBinaryHeader(token.length);
            packer.writePayload(token);
            packer.flush();
            return outputStream.toByteArray();
        }
    }

    @Override
    public byte[] unpack(byte[] packedToken)
            throws IOException
    {
        synchronized (unpacker) {
            MessageBufferInput bufferInput = new ArrayBufferInput(packedToken);
            unpacker.reset(bufferInput);
            int mapLen = unpacker.unpackMapHeader();
            byte[] unpackedToken = null;
            for (int i = 0; i < mapLen; i++) {
                if (unpacker.unpackString().equals("ack")) {
                    unpackedToken = new byte[unpacker.unpackBinaryHeader()];
                    unpacker.readPayload(unpackedToken);
                    break;
                }
            }
            return unpackedToken;
        }
    }
}
