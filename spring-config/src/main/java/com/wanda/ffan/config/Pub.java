package com.wanda.ffan.config;
/**
 * 
 * @author zhangling
 *
 */
public class Pub {
	private String id;
	 public String getId() {
		return id;
	}



	public void setId(String id) {
		this.id = id;
	}



	/**
     * 生产者应用ID，用作身份验证
     */
    private String appId;
    /**
     * AppSecret，用作授权
     */
    private String key;

    /**
     * 发送地址 (示例:http://10.213.57.148:10191/topics/default-owl-biz/v1)
     */
    private String url;
    
    

	public String getAppId() {
		return appId;
	}



	public void setAppId(String appId) {
		this.appId = appId;
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



	public Pub(String id, String appId, String key, String url) {
		super();
		this.id = id;
		this.appId = appId;
		this.key = key;
		this.url = url;
	}



	public Pub() {
		super();
		// TODO Auto-generated constructor stub
	}
	
    
    
}
