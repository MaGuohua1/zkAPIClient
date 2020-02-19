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
import org.apache.zookeeper.data.Stat;

public class CreateSession implements Watcher {

	private static CountDownLatch latch = new CountDownLatch(1);
	private static final String CONNECTION_STR = "192.168.1.104:2181";

	public static void main(String[] args) {
		CreateSession createSession = new CreateSession();
		createSession.test();
	}

	private void test() {
		try {
			// 创建zk
			ZooKeeper zooKeeper = new ZooKeeper(CONNECTION_STR, 5000, this);
			latch.await();
			System.out.println(zooKeeper.getState());

			// 创建一个节点
			// 节点的路径，节点值，节点的acl权限，节点类型
			zooKeeper.create("/node1", "node1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

			// 获取数据
			Stat stat = new Stat();
			// watch只触发一次，如果想再次触发，则在执行getData一次
			byte[] data = zooKeeper.getData("/node1", true, stat);
			System.out.println(new String(data));
			System.out.println(stat);

			// 修改数据
			Stat stat2 = zooKeeper.setData("/node1", "node".getBytes(), -1);
			data = zooKeeper.getData("/node1", true, stat);
			System.out.println(new String(data));
			System.out.println(stat2);

			// 删除节点
			zooKeeper.delete("/node1", -1);

			// 查询节点
			List<String> list = zooKeeper.getChildren("/node", true);
			System.out.println(list);
		} catch (IOException | InterruptedException | KeeperException e) {
			e.printStackTrace();
		}

	}

	public void process(WatchedEvent event) {
		if (event.getState() == Event.KeeperState.SyncConnected && event.getType() == Event.EventType.None) {
			latch.countDown();
			System.out.println(event.getState());
		}
		if (event.getType() == Event.EventType.NodeDataChanged) {
			System.out.println("数据发生变更,路径是：" + event.getPath());
		} else if (event.getType() == Event.EventType.NodeDeleted) {
			System.out.println("数据发生删除,路径是：" + event.getPath());
		}
	}
}
