package com.wanda.ffan.config;
/**
 * 
 * @author zhangling
 *
 */
public class Sub {
	private String id;
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	/**
	 * 消费者应用ID，用作身份验证
	 */
	private String appId;
	/**
	 * AppSecret，用作授权
	 */
	private String key;

	/**
	 * 接收地址
	 */
	private String url;

	/**
	 * SUB端配置
	 *
	 * @param subUrl
	 *            数据接收地址
	 * @param appId
	 *            生产者应用ID，用作身份验证
	 * @param subKey
	 *            AppSecret，用作授权
	 */
	public Sub(String url, String appId, String key) {
		this.url = url;
		this.appId = appId;
		this.key = key;
	}

	public String getAppId() {
		return appId;
	}

	public void setAppId(String appId) {
		this.appId = appId;
	}
	
	
	public Sub(String id, String appId, String key, String url) {
		super();
		this.id = id;
		this.appId = appId;
		this.key = key;
		this.url = url;
	}

	
	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public Sub() {
		super();
		// TODO Auto-generated constructor stub
	}
	
}
