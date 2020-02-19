package com.ma.curator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

/**
 * PathChildrenCache 监视一个路径下子节点的创建删除 
 * NodeCache 监视一个节点的更新 
 * TreeCache PathChildrenCache+NodeCache的合体（监视路径下的创建、更新、删除事件）
 * 
 * @author mgh_2
 *
 * @desription
 */
public class CuratorOperator {

	public static void main(String[] args) {
		CuratorOperator operator = new CuratorOperator();
		operator.operate();
	}

	@SuppressWarnings({ "deprecation", "resource" })
	private void operate() {
		CuratorFramework curator = Session.getInstance();
		try {

			// 创建节点
			String path = curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
					.forPath("/aaa/bbb/ccc/ddd", "aaa".getBytes());
			System.out.println(path);

			// ============================================================
			// watcher NodeCache
			NodeCache nodeCache = new NodeCache(curator, path);
			nodeCache.start(true);// true代表缓存当前节点
			if (nodeCache.getCurrentData() != null) {// 只有start中的设置为true才能够直接得到
				System.out.println("nodeCache-------CurrentNode Data is:"
						+ new String(nodeCache.getCurrentData().getData().toString()) + "\n===========================\n");// 输出当前节点的内容
			}
			nodeCache.getListenable().addListener(() -> nodeChanged(nodeCache));
			
			// watcher PathChildrenCache
			PathChildrenCache childrenCache = new PathChildrenCache(curator, "/aaa/bbb/ccc", false);
			childrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
			if (!childrenCache.getCurrentData().isEmpty()) {// 只有start中的设置为true才能够直接得到
				for (ChildData childData : childrenCache.getCurrentData()) {
					System.out.println("childrenCache-------CurrentNode Data is:"
							+ new String(childData.getData()) + "\n===========================\n");// 输出当前节点的内容
				}
			}
			childrenCache.getListenable().addListener((client, event) -> childEvent(event));
			
			// watcher TreeCache
			TreeCache treeCache = new TreeCache(curator, "/aaa");
			treeCache.start();
			treeCache.getListenable().addListener((client, event) -> childEvent(event));
			// ============================================================

			// 查询节点
			Stat stat = new Stat();
			byte[] data = curator.getData().storingStatIn(stat).forPath("/aaa/bbb/ccc/ddd");
			System.out.println(stat);
			System.out.println(new String(data));

			// 修改节点
			curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/aaa/bbb/ccc/eeee",
					"eeee".getBytes());
			stat = curator.setData().forPath("/aaa/bbb/ccc", "ccc".getBytes());
			stat = curator.setData().forPath("/aaa/bbb/ccc/ddd", "vvvv".getBytes());
			stat = curator.setData().forPath("/aaa/bbb/ccc/ddd", "bbbb".getBytes());
			System.out.println(stat);

			// 删除节点
			curator.delete().deletingChildrenIfNeeded().forPath("/aaa");

		} catch (Exception e) {
			e.printStackTrace();
		}

		// 异步
		ExecutorService executor = Executors.newSingleThreadExecutor();
		CountDownLatch latch = new CountDownLatch(1);
		try {
			curator.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
					.inBackground((client, event) -> proessResult(client, event, latch), executor)
					.forPath("/node", "node".getBytes());
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			latch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			executor.shutdown();
		}

		// 事务1
		try {
			Collection<CuratorTransactionResult> results = curator.inTransaction().create()
					.withMode(CreateMode.EPHEMERAL).forPath("/mmm", "mmm".getBytes()).and().setData()
					.forPath("/mmm", "lll".getBytes()).and().commit();

			for (CuratorTransactionResult result : results) {
				System.out.println(result.getForPath() + ":" + result.getType());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		// 事务2
		try {
			List<CuratorOp> operations = new ArrayList<>();
			CuratorOp create = curator.transactionOp().create().forPath("/oooo", "oooo".getBytes());
			CuratorOp set = curator.transactionOp().setData().forPath("/oooo", "pppp".getBytes());
			CuratorOp delete = curator.transactionOp().delete().forPath("/oooo");
			operations.add(create);
			operations.add(set);
			operations.add(delete);
			List<CuratorTransactionResult> list = curator.transaction().forOperations(operations);
			for (CuratorTransactionResult result : list) {
				System.out.println(result.getForPath() + ":" + result.getType());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void childEvent(TreeCacheEvent event) {
		switch (event.getType()) {
		case NODE_ADDED:
			System.out.println("增加节点" + event.getData().getPath());
			break;
		case NODE_UPDATED:
			System.out.println("修改节点" + event.getData().getPath() + " -> 数据：" + event.getData().getData().toString());
			break;
		case NODE_REMOVED:
			System.out.println("删除节点" + event.getData().getPath());
			break;
		default:
			break;
		}
	}

	private void childEvent(PathChildrenCacheEvent event) {
		switch (event.getType()) {
		case CHILD_ADDED:
			System.out.println("增加子节点" + event.getData().getPath());
			break;
		case CHILD_UPDATED:
			System.out.println("修改子节点" + event.getData().getPath() + " -> 数据：" + new String(event.getData().getData()));
			break;
		case CHILD_REMOVED:
			System.out.println("删除子节点" + event.getData().getPath());
			break;
		default:
			break;
		}
	}

	public void nodeChanged(NodeCache nodeCache) throws Exception {
		System.out.println(
				"nodeCache------节点数据发生了改变，发生的路径为：" + nodeCache.getCurrentData().getPath() + ",节点数据发生了改变 ，新的数据为："
						+ new String(nodeCache.getCurrentData().getData()) + "\n===========================\n");
	}

	private void proessResult(CuratorFramework client, CuratorEvent event, CountDownLatch latch) {
		System.out.println(Thread.currentThread().getName() + " -> resultCode: " + event.getResultCode() + " -> type: "
				+ event.getType());
		latch.countDown();
	}
}
