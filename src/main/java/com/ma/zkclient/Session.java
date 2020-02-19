package com.ma.zkclient;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.ZkClient;

public class Session {

	private static final String CONNECTION = "192.168.1.104:2181";

	public static void main(String[] args) throws InterruptedException {
		ZkClient client = new ZkClient(CONNECTION, 1000);
		client.deleteRecursive("/qqq");// 删除

		client.createPersistent("/qqq/www/eee/rrr", true);// 创建节点
		List<String> children = client.getChildren("/qqq");// 获取子节点
		System.out.println(children);

		Zklistener listener = new Zklistener();// watcher
		// 节点数据改变后触发,(监听最新的值)
		client.subscribeDataChanges("/qqq", listener);
		client.writeData("/qqq", "asdf");
		Object o = client.readData("/qqq");
		System.out.println(o);
		TimeUnit.SECONDS.sleep(2);
		client.writeData("/qqq", "asdfgh");
		// 节点的子节点改变后（create,delete）触发
		client.subscribeChildChanges("/qqq", listener);
		client.deleteRecursive("/qqq/www");// 删除
		client.createPersistent("/qqq/aaa/sss/ddd", true);// 创建节点
	}
}
