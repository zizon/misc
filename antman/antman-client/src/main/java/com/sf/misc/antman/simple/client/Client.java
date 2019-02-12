package com.sf.misc.antman.simple.client;

import com.sf.misc.antman.Promise;

import java.io.File;
import java.net.SocketAddress;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * upload client
 */
public class Client {

    /**
     * session state listeners
     */
    public static interface SessionListener extends IOContext.ProgressListener {
    }

    /**
     * upload session.
     */
    public static class Session {

        protected IOContext context;
        protected Promise.PromiseSupplier<SocketAddress> server_provider;
        protected AtomicBoolean success;

        protected Session(File file, UUID stream_id, Promise.PromiseSupplier<SocketAddress> server_provider, IOContext.ProgressListener listener, TuningParameters parameters) {
            this.server_provider = server_provider;
            this.success = new AtomicBoolean(false);
            this.context = new IOContext(
                    file,
                    stream_id,
                    parameters.chunk(),
                    parameters.ackTimeout(),
                    new IOContext.ProgressListener() {
                        @Override
                        public void onRangeFail(long offset, long size, Throwable reason) {
                            listener.onRangeFail(offset, size, reason);
                        }

                        @Override
                        public void onUploadSuccess() {
                            listener.onUploadSuccess();
                            success.set(true);
                        }

                        @Override
                        public void onChannelComplete() {
                            listener.onChannelComplete();
                        }

                        @Override
                        public void onProgress(long commited, long failed, long going, long expected) {
                            listener.onProgress(commited, failed, going, expected);
                        }
                    }
            );
        }

        /**
         * try upload bounded file to server,may partial completed
         *
         * @return the future object that will be resolve on uplaod produce complete(either success or with failure)
         */
        public Future<?> tryUpload() {
            return new IOChannel(server_provider.get(), this.context).upload();
        }


        /**
         * flags that if this session uplaod is complete,
         * if not ,use try upload to try again
         *
         * @return ture if server commited
         */
        public boolean isAllCommit() {
            return success.get();
        }
    }


    protected final SocketAddress server;

    /**
     * the server to connected to
     *
     * @param server remote server
     */
    public Client(SocketAddress server) {
        this.server = server;
    }

    /**
     * upload file to server
     *
     * @param file       the file to be upload
     * @param stream_id  uniq id for this file in universe.
     * @param listener   various state listener
     * @param parameters tuning parameter that contorll upload behavior
     * @return the session object
     */
    public Session upload(File file, UUID stream_id, SessionListener listener, TuningParameters parameters) {
        return new Session(file, stream_id, this::candidateServer, listener, parameters);
    }

    protected SocketAddress candidateServer() {
        return server;
    }
}
