package com.sf.misc.antman.simple.client;

import com.sf.misc.antman.Promise;
import com.sf.misc.antman.simple.packets.RequestCRCAckPacket;
import com.sf.misc.antman.simple.packets.RequestCRCPacket;
import io.netty.channel.ChannelFuture;

import javax.swing.*;
import java.util.Map;
import java.util.NavigableSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

public interface FileChunkAcks {

    static interface Range extends Comparable<Range> {

        public UUID stream();

        public long from();

        public long to();

        default public int compareTo(Range o) {
            int match = stream().compareTo(o.stream());
            if (match != 0) {
                return match;
            }

            match = Long.compare(from(), o.from());
            if (match != 0) {
                return match;
            }

            match = Long.compare(from(), to());
            return match;
        }
    }

    default public Promise<Long> rangeAckedCRC(UUID stream, long from, long to) {
        return ackedRanges().computeIfAbsent(
                newRange(stream, from, to),
                (range) -> {
                    return requestCRC(
                            new RequestCRCPacket(
                                    range.stream(),
                                    range.from(),
                                    range.to() - range.from()
                            )
                    );
                });
    }

    default public Range newRange(UUID strem, long from, long to) {
        return new Range() {
            @Override
            public UUID stream() {
                return strem;
            }

            @Override
            public long from() {
                return from;
            }

            @Override
            public long to() {
                return to;
            }
        };
    }

    default public void onReceiveAck(RequestCRCAckPacket packet) {
        UUID stream = packet.getStream();
        long from = packet.getOffset();
        long to = from + packet.getLength();
        long crc = packet.getCrc();

        // create range
        ackedRanges().compute(newRange(stream, from, to), (range, old) -> {
            if (old == null) {
                return Promise.success(crc);
            }

            old.complete(crc);
            return old;
        });
    }

    default public Promise<Long> requestCRC(RequestCRCPacket range) {
        return ackedRanges().computeIfAbsent(
                newRange(range.getStream(),
                        range.getOffset(),
                        range.getOffset() + range.getLength()
                ),
                (key) -> {
                    return Promise.promise();
                });
    }


    public ConcurrentMap<Range, Promise<Long>> ackedRanges();
}
