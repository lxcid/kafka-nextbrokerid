package com.lxcid.kafka;

import java.lang.InterruptedException;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

public class App {
    public static void main(String[] args) {
        Options options = new Options();

        Option zkcOpt = new Option("zkc", "zookeeper-connect", true, "Zookeeper Connect");
        zkcOpt.setRequired(true);
        options.addOption(zkcOpt);

        Option timeoutOpt = new Option("t", "timeout", true, "Timeout in milliseconds (Default to 30000)");
        options.addOption(timeoutOpt);

        Option startOpt = new Option("s", "start", true, "First Broker ID (Default to 1)");
        options.addOption(startOpt);

        Option totalOpt = new Option("tt", "total", true, "Total possible IDs (Default to 100)");
        options.addOption(totalOpt);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("nextbrokerid", options);
            
            System.exit(1);
            return;
        }
        
        String zookeeperConnect = cmd.getOptionValue("zookeeper-connect");
        int timeout = Integer.parseInt(cmd.getOptionValue("timeout", "30000"));
        int start = Integer.parseInt(cmd.getOptionValue("start", "1"));
        int total = Integer.parseInt(cmd.getOptionValue("total", "100"));

        try {
            final CountDownLatch connSignal = new CountDownLatch(1);
            ZooKeeper zookeeper = new ZooKeeper(zookeeperConnect, timeout, new Watcher() {
                public void process(WatchedEvent event) {
                    if (event.getState() == KeeperState.SyncConnected) {
                        connSignal.countDown();
                    }
                }
            });
            if (!connSignal.await(timeout, TimeUnit.MILLISECONDS)) {
                throw new TimeoutException("Timeout while connecting to " + zookeeperConnect + ".");
            }
            List<String> usedIDs = zookeeper.getChildren("/brokers/ids", false);
            HashSet<String> possibleIDs = new HashSet<String>(100);
            for (int i = start; i < start + total; i++) {
                possibleIDs.add(Integer.toString(i));
            }
            possibleIDs.removeAll(usedIDs);
            List<String> availableIDs = new ArrayList<String>(possibleIDs);
            Collections.sort(availableIDs);
            String nextBrokerID = availableIDs.get(0);
            System.out.println(nextBrokerID);
        } catch (IOException|InterruptedException|TimeoutException|KeeperException e) {
            System.out.println(e.getMessage());
            
            System.exit(1);
            return;
        }
    }
}
