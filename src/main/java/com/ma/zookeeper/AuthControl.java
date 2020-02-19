package com.ma.zookeeper;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;

public class AuthControl {

	private static CountDownLatch latch = new CountDownLatch(1);
	private static CountDownLatch latch2 = new CountDownLatch(1);
	private static final String CONNECTION_STR = "192.168.1.104:2181";

	public static void main(String[] args) {
		AuthControl control = new AuthControl();
		control.test();
	}

	private void test() {
		try {
			ZooKeeper zooKeeper = new ZooKeeper(CONNECTION_STR, 5000, event -> process(event));
			latch.await();

			String user = "user:12345";
			ACL acl = new ACL(Perms.CREATE | Perms.DELETE | Perms.READ | Perms.WRITE | Perms.ADMIN,
					new Id("digest", DigestAuthenticationProvider.generateDigest(user)));
			List<ACL> list = new ArrayList<ACL>();
			list.add(acl);

			zooKeeper.addAuthInfo("digest", user.getBytes());
			zooKeeper.create("/node", "node".getBytes(), list, CreateMode.PERSISTENT);
			zooKeeper.create("/node/node1", "123".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

			ZooKeeper zooKeeper2 = new ZooKeeper(CONNECTION_STR, 5000, event -> process2(event));
			latch2.await();
			zooKeeper2.addAuthInfo("digest", user.getBytes());
			zooKeeper2.delete("/node/node1", -1);

		} catch (IOException | InterruptedException | NoSuchAlgorithmException | KeeperException e) {
			e.printStackTrace();
		}
	}

	private void process(WatchedEvent event) {
		if (event.getState() == Event.KeeperState.SyncConnected && event.getType() == Event.EventType.None) {
			latch.countDown();
			System.out.println("1111" + event.getState());
		}
	}

	private void process2(WatchedEvent event) {
		if (event.getState() == Event.KeeperState.SyncConnected && event.getType() == Event.EventType.None) {
			System.out.println("2222" + event.getState());
			latch2.countDown();
		}
	}

}
