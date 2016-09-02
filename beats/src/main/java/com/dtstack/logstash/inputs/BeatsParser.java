package com.dtstack.logstash.inputs;


import com.fasterxml.jackson.databind.ObjectMapper;
//import com.fasterxml.jackson.module.afterburner.AfterburnerModule;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.InflaterInputStream;


public class BeatsParser extends ByteToMessageDecoder {
    private static final int CHUNK_SIZE = 1024;
    private final static Logger logger = LoggerFactory.getLogger(BeatsParser.class);

    private Batch batch = new Batch();

    private enum States {
        READ_HEADER,
        READ_FRAME_TYPE,
        READ_WINDOW_SIZE,
        READ_JSON_HEADER,
        READ_COMPRESSED_FRAME_HEADER,
        READ_COMPRESSED_FRAME,
        READ_JSON,
        READ_DATA_FIELDS,
    }

    private States currentState = States.READ_HEADER;
    private long requiredBytes = 0;
    private int sequence = 0;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if(!hasEnoughBytes(in)) {
            return;
        }

        switch (currentState) {
            case READ_HEADER: {
                logger.debug("Running: READ_HEADER");

                byte currentVersion = in.readByte();

                if(Protocol.isVersion2(currentVersion)) {
                    logger.debug("Frame version 2 detected");
                    batch.setProtocol(Protocol.VERSION_2);
                } else {
                    logger.debug("Frame version 1 detected");
                    batch.setProtocol(Protocol.VERSION_1);
                }

                transition(States.READ_FRAME_TYPE, 1);
                break;
            }
            case READ_FRAME_TYPE: {
                logger.debug("Running: READ_FRAME_TYPE");
                byte frameType = in.readByte();

                switch(frameType) {
                    case Protocol.CODE_WINDOW_SIZE: {
                        transition(States.READ_WINDOW_SIZE, 4);
                        break;
                    }
                    case Protocol.CODE_JSON_FRAME: {
                        // Reading Sequence + size of the payload
                        transition(States.READ_JSON_HEADER, 8);
                        break;
                    }
                    case Protocol.CODE_COMPRESSED_FRAME: {
                        transition(States.READ_COMPRESSED_FRAME_HEADER, 4);
                        break;
                    }
                    case Protocol.CODE_FRAME: {
                        transition(States.READ_DATA_FIELDS, 8);
                        break;
                    }
                }
                break;
            }
            case READ_WINDOW_SIZE: {
                logger.debug("Running: READ_WINDOW_SIZE");
                this.batch.setWindowSize((int) in.readUnsignedInt());

                // This is unlikely to happen but I have no way to known when a frame is
                // actually completely done other than checking the windows and the sequence number,
                // If the FSM read a new window and I have still
                // events buffered I should send the current batch down to the next handler.
                if(!this.batch.isEmpty()) {
                    logger.warn("New window size received but the current batch was not complete, sending the current batch");
                    out.add(this.batch);
                    this.batchComplete();
                }

                transitionToReadHeader();
                break;
            }
            case READ_DATA_FIELDS: {
                // Lumberjack version 1 protocol, which use the Key:Value format.
                logger.debug("Running: READ_DATA_FIELDS");
                this.sequence = (int) in.readUnsignedInt();
                int fieldsCount = (int) in.readUnsignedInt();
                int count = 0;

                Map dataMap = new HashMap<String, String>();

                while(count < fieldsCount) {
                    int fieldLength = (int) in.readUnsignedInt();
                    String field = in.readBytes(fieldLength).toString(Charset.forName("UTF8"));

                    int dataLength = (int) in.readUnsignedInt();
                    String data = in.readBytes(dataLength).toString(Charset.forName("UTF8"));

                    dataMap.put(field, data);

                    count++;
                }

                Message message = new Message(sequence, dataMap);
                this.batch.addMessage(message);

                if(this.batch.size() == this.batch.getWindowSize()) {
                    out.add(batch);
                    this.batchComplete();
                }

                transitionToReadHeader();

                break;
            }
            case READ_JSON_HEADER: {
                logger.debug("Running: READ_JSON_HEADER");

                this.sequence = (int) in.readUnsignedInt();
                int jsonPayloadSize = (int) in.readUnsignedInt();

                transition(States.READ_JSON, jsonPayloadSize);
                break;
            }
            case READ_COMPRESSED_FRAME_HEADER: {
                logger.debug("Running: READ_COMPRESSED_FRAME_HEADER");

                transition(States.READ_COMPRESSED_FRAME, in.readUnsignedInt());
                break;
            }

            case READ_COMPRESSED_FRAME: {
                logger.debug("Running: READ_COMPRESSED_FRAME");


                byte[] bytes = new byte[(int) this.requiredBytes];
                in.readBytes(bytes);

                InputStream inflater = new InflaterInputStream(new ByteArrayInputStream(bytes));
                ByteArrayOutputStream decompressed = new ByteArrayOutputStream();

                byte[] chunk = new byte[CHUNK_SIZE];
                int length = 0;

                while ((length = inflater.read(chunk)) > 0) {
                    decompressed.write(chunk, 0, length);
                }

                inflater.close();
                decompressed.close();

                transitionToReadHeader();
                ByteBuf newInput = Unpooled.wrappedBuffer(decompressed.toByteArray());
                while(newInput.readableBytes() > 0) {
                    decode(ctx, newInput, out);
                }

                break;
            }
            case READ_JSON: {
                logger.debug("Running: READ_JSON");

                ByteBuf buffer = in.readBytes((int) this.requiredBytes);
                Message message = new Message(sequence, (Map) JsonUtils.mapper.readValue(buffer.array(), Object.class));

                this.batch.addMessage(message);

                if(this.batch.size() == this.batch.getWindowSize()) {
                    out.add(this.batch);
                    this.batchComplete();
                }

                transitionToReadHeader();
                break;
            }
        }
    }

    private boolean hasEnoughBytes(ByteBuf in) {
        if(in.readableBytes() >= this.requiredBytes) {
            return true;
        }
        return false;
    }

    public void transitionToReadHeader() {
        transition(States.READ_HEADER, 1);
    }

    public void transition(States next, long need) {
        logger.debug("Transition, from: " + this.currentState + " to: " + next + " required bytes: " + need);
        this.currentState = next;
        this.requiredBytes = need;
    }

    public void batchComplete() {
        this.requiredBytes = 0;
        this.sequence = 0;
        this.batch = new Batch();
    }
}