package com.sf.misc.antman.simple.client;

import com.sf.misc.antman.Promise;
import com.sf.misc.antman.simple.CRC;
import com.sf.misc.antman.simple.MemoryMapUnit;
import com.sf.misc.antman.simple.packets.CommitStreamPacket;
import com.sf.misc.antman.simple.packets.StreamChunkPacket;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

public class IOChannel {

    protected final IOHandler handler;

    public IOChannel(IOHandler handler) {
        this.handler = handler;
    }

    public void upload(IOContext context) {
        Promise<?> sent = Promise.costly(() -> send(context));
        Promise<?> receive = Promise.costly(() -> receive(context));

        Promise<?> done_crc = sent.transformAsync((ignore) -> {
            return doCRC(context);
        });

        // clean up
        done_crc.transform((ignore) -> {
            context.onChannelComplete();
            return null;
        }).catching((throwable) -> {
            context.onAbortChannel(throwable);
        }).addListener(() -> {
            handler.close();
        });

        // hook receive
        
    }

    public Promise<?> doCRC(IOContext context) {
        // local crc
        Promise<Long> local_crc = mmu().map(context.file(), 0, context.file().length())
                .transform((buffer) -> {
                    return CRC.crc(buffer);
                });

        // sent crc
        Promise<?> request_crc = local_crc.transformAsync((crc) -> {
            return handler.write(new CommitStreamPacket(context.streamID(), 0, crc));
        });

        // remote crc
        Promise<Long> remote_crc = request_crc.costly(() -> {
            return handler.readCRC();
        });

        // notify
        return Promise.all(local_crc, remote_crc).transform((ignore) -> {
            if (local_crc.join() == remote_crc.join()) {
                context.onSuccess();
            } else {
                context.onCRCNotMatch(local_crc.join(), remote_crc.join());
            }

            return null;
        });
    }

    public void receive(IOContext context) {
        for (Optional<IOContext.Range> range = handler.read(); range != null; range = handler.read()) {
            if (!range.isPresent()) {
                // EOF
                break;
            }

            // ack
            range.ifPresent(context::onCommit);
        }
    }

    public void send(IOContext context) {
        // write ok
        context.pending().stream()
                .forEach((range) -> {
                    Promise<ByteBuffer> buffer = mmu().map(context.file(), range.offset(), range.size());
                    Promise<StreamChunkPacket> packet = buffer.transform((content) -> new StreamChunkPacket(context.streamID(), range.offset(), content));

                    // sent
                    Promise<?> sent = packet.transformAsync((content) -> handler.write(content));

                    // clean up
                    Promise.all(buffer, sent).addListener(() -> {
                        mmu().unmap(buffer.join());
                    });

                    // add timeout
                    Promise<?> with_ack_timeout = sent.transformAsync((ignore) -> {
                        Promise<?> timeout = Promise.promise();

                        // register going
                        context.going(range, timeout);

                        return timeout.timeout(() -> {
                            throw new TimeoutException("stream:" + context.streamID() + " of range:" + range + " wait ack timeout:" + context.timeout());
                        }, context.timeout());
                    });

                    // listen
                    with_ack_timeout.catching((throwable) -> {
                        if (throwable instanceof TimeoutException) {
                            context.onTimeout(range);
                        } else {
                            context.onFailure(range, throwable);
                        }
                    });
                });
    }

    public MemoryMapUnit mmu() {
        return MemoryMapUnit.shared();
    }

}
