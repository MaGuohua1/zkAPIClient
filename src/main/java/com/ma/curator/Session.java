package com.ma.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class Session {

	private static CuratorFramework client;
	private static final String connectString = "192.168.1.104:2181";

	public static CuratorFramework getInstance() {
		// 连接策略: 首次连接时间1000ms, 重试连接3次
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		int sessionTimeoutMs = 3000;
		int connectionTimeoutMs = 3000;
		client = CuratorFrameworkFactory.builder().connectString(connectString)
				.connectionTimeoutMs(connectionTimeoutMs).sessionTimeoutMs(sessionTimeoutMs).retryPolicy(retryPolicy)
				.build();
		client.start();// start方法启动连接
		return client;
	}
}
