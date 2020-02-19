package com.ma.zkclient;

import java.util.List;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;

public class Zklistener implements IZkDataListener, IZkChildListener {

	@Override
	public void handleDataChange(String dataPath, Object data) throws Exception {
		System.out.println("节点名称：" + dataPath + " -> 节点修改后的值" + data);
	}

	@Override
	public void handleDataDeleted(String dataPath) throws Exception {
		System.out.println("被删除的节点是" + dataPath);
	}

	@Override
	public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
		System.out.println("节点名称：" + parentPath + " -> 节点子列表：" + currentChilds);
	}

}
