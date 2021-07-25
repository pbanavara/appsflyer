package com.appsflyer.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.async.*;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.ClientPolicy;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class Write {

    public static void main(String[] args) {
        int numberOfRecords;
        int maxCommands;
        try {
            if (args.length < 2) {
                System.out.println("Using default values for threads and number of records");
                numberOfRecords = 2000;
                maxCommands = 40;
            } else {
                numberOfRecords = Integer.parseInt(args[0]);
                maxCommands = Integer.parseInt(args[1]);
            }
            Write test = new Write(numberOfRecords, maxCommands);
            test.runTest();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private AerospikeClient client;
    private EventLoops eventLoops;
    private final Monitor monitor = new Monitor();
    private final AtomicInteger recordCount = new AtomicInteger();
    private final int maxCommandsInProcess;
    private final int recordMax = 1;
    private final int writeTimeout = 5000;
    private final int eventLoopSize;
    private final int concurrentMax;
    private final int numberOfRecords;

    private String HOST;
    private int PORT;
    public Write(int numRecords, int maxCommands) {
        // Allocate an event loop for each cpu core.
        eventLoopSize = Runtime.getRuntime().availableProcessors();
        Map<String, String> envs = System.getenv();
        for (String envName: envs.keySet()) {
            if (envName.equals("AEROSPIKE_HOST")) {
                HOST = envs.get(envName);
            }
            if (envName.equals("AEROSPIKE_PORT")) {
                PORT = Integer.parseInt(envs.get(envName));
            }

        }
        numberOfRecords = numRecords;
        maxCommandsInProcess = maxCommands;
        // Set total concurrent commands for all event loops.
        concurrentMax = eventLoopSize * maxCommandsInProcess;
    }

    public void runTest() throws AerospikeException, Exception {
        EventPolicy eventPolicy = new EventPolicy();
        eventPolicy.minTimeout = writeTimeout;

        // This application uses it's own external throttle (Start with concurrentMax
        // commands and only start one new command after previous command completes),
        // so setting EventPolicy maxCommandsInProcess is not necessary.
        // eventPolicy.maxCommandsInProcess = maxCommandsInProcess;

        // Direct NIO
        eventLoops = new NioEventLoops(eventPolicy, eventLoopSize);

        // Netty NIO
        // EventLoopGroup group = new NioEventLoopGroup(eventLoopSize);
        // eventLoops = new NettyEventLoops(eventPolicy, group);

        // Netty epoll (Linux only)
        // EventLoopGroup group = new EpollEventLoopGroup(eventLoopSize);
        // eventLoops = new NettyEventLoops(eventPolicy, group);

        try {
            ClientPolicy clientPolicy = new ClientPolicy();
            clientPolicy.eventLoops = eventLoops;

            // maxConnsPerNode needs to be increased from default (300)
            // if eventLoopSize >= 8.
            clientPolicy.maxConnsPerNode = concurrentMax;

            clientPolicy.writePolicyDefault.setTimeout(writeTimeout);

            client = new AerospikeClient(clientPolicy, HOST, PORT);

            try {
                writeRecords();
                monitor.waitTillComplete();
                System.out.println("Records written: " + recordCount.get());
            }
            finally {
                client.close();
            }
        }
        finally {
            eventLoops.close();
        }
    }

    private void writeRecords() {
        // Write exactly concurrentMax commands to seed event loops.
        // Distribute seed commands across event loops.
        // A new command will be initiated after each command completion in WriteListener.
        for (int i = 1; i < numberOfRecords; i++) {
            EventLoop eventLoop;
            eventLoop = eventLoops.next();
            writeRecord(eventLoop, new AWriteListener(eventLoop), i);
        }
    }

    private void writeRecord(EventLoop eventLoop, WriteListener listener, int keyIndex) {
        Key key = new Key("test", "test", keyIndex);
        int byteCount = 16;
        byte[] data = new byte[byteCount];
        for (int i = 0; i < byteCount; ++i) {
            data[i] = new Integer(i).byteValue();
        }
        Bin bin = new Bin("singleBin", data);
        client.put(eventLoop, listener, null, key, bin);
    }

    private class AWriteListener implements WriteListener {
        private final EventLoop eventLoop;

        public AWriteListener(EventLoop eventLoop) {
            this.eventLoop = eventLoop;
        }

        @Override
        public void onSuccess(Key key) {

        }

        @Override
        public void onFailure(AerospikeException e) {
            e.printStackTrace();
            monitor.notifyComplete();
        }
    }
}
