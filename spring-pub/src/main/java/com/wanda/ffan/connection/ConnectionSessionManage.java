package com.wanda.ffan.connection;

import org.apache.http.client.HttpClient;

public interface ConnectionSessionManage {
	 HttpClient createHttpConnection(String name);
	 
}
