package com.lxcid.kafka;

import java.lang.InterruptedException;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

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

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("utility-name", options);
            
            System.exit(1);
            return;
        }
        
        String zookeeperConnect = cmd.getOptionValue("zookeeper-connect");

        final CountDownLatch connSignal = new CountDownLatch(1);

        try {
            System.out.println("Connecting to " + zookeeperConnect + "…");
            ZooKeeper zk = new ZooKeeper(zookeeperConnect, 30000, new Watcher() {
                public void process(WatchedEvent event) {
                    if (event.getState() == KeeperState.SyncConnected) {
                        connSignal.countDown();
                    }
                }
            });
            if (!connSignal.await(30000, TimeUnit.MILLISECONDS)) {
                throw new TimeoutException("Timeout while connecting to " + zookeeperConnect + ".");
            }
            System.out.println("Connected to " + zookeeperConnect + ".");

            List<String> ids = zk.getChildren("/brokers/ids", false);
            System.out.println(ids);
        } catch (IOException|InterruptedException|TimeoutException|KeeperException e) {
            System.out.println(e.getMessage());
            
            System.exit(1);
            return;
        }
    }
}
