package com.ma.zookeeper;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

public class Cascade implements Watcher {

	private static final String CONN_STR = "192.168.1.104:2181";
	private static CountDownLatch latch = new CountDownLatch(1);

	public static void main(String[] args) {
		Cascade cascade = new Cascade();
		cascade.cascade();
	}

	private void cascade() {
		try {
			ZooKeeper keeper = new ZooKeeper(CONN_STR, 3000, this);
			latch.await();
			createCasCade(keeper, "/node/aaa/bbb");
			deleteCascade(keeper, "/");
		} catch (IOException | InterruptedException | KeeperException e) {
			e.printStackTrace();
		}
	}

	private void createCasCade(ZooKeeper keeper, String path) throws KeeperException, InterruptedException {
		int endIndex = 0;
		if ((endIndex = path.lastIndexOf("/")) != 0 && keeper.exists(path, true) == null) {
			String subPath = path.substring(0, endIndex);
			createCasCade(keeper, subPath);
		}
		keeper.create(path, path.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	}

	private void deleteCascade(ZooKeeper keeper, String path) throws KeeperException, InterruptedException {
		List<String> list = null;
		if (!(list = keeper.getChildren(path, true)).isEmpty()) {
			for (String node : list) {
				String subPath = null;
				if ("/".equals(path)) {
					subPath = path + node;
				} else {
					subPath = path + "/" + node;
				}
				if (!"/zookeeper".equals(subPath)) {
					deleteCascade(keeper, subPath);
				}
			}
		}
		if (!"/".equals(path)) {
			keeper.delete(path, -1);
		}
	}

	@Override
	public void process(WatchedEvent event) {
		if (event.getState() == Event.KeeperState.SyncConnected && event.getType() == Event.EventType.None) {
			latch.countDown();
		}
	}
}
