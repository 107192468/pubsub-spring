package com.wanda.ffan.pub;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.wanda.ffan.common.Constants;
import com.wanda.ffan.common.utils.NumberUtils;
import com.wanda.ffan.common.utils.StringUtils;
import com.wanda.ffan.config.Http;
import com.wanda.ffan.config.Pub;
import com.wanda.ffan.enums.StatusCodeEnum;
import com.wanda.ffan.exception.AuthorityErrorException;

public class HandleMassege implements Handler {
	  private static final Logger logger = LoggerFactory.getLogger(HandleMassege.class);
		
		private Http http;
		private Pub pub;
		private CloseableHttpClient httpClient;
		public Http getHttp() {
			return http;
		}
		public void setHttp(Http http) {
			this.http = http;
		}
		public Pub getPub() {
			return pub;
		}
		public void setPub(Pub pub) {
			this.pub = pub;
		}
		
		
		
		public CloseableHttpClient getHttpClient() {
			return httpClient;
		}
		public void setHttpClient(CloseableHttpClient httpClient) {
			this.httpClient = httpClient;
		}
		public void send(String messageKey, String message){
			send(messageKey,message,0);
		}
		
		
		/**
		 * 发送消息
		 *
		 * @param messageKey
		 *            msgkey 按消息Key排序
		 * @param message
		 *            消息内容
		 * @return 发送结果
		 */
		private  PubResult send(String messageKey, String message,int retry) {
			if ("".equals(message) || message == null) {
				throw new RuntimeException("message can not be null");
			}
			String pubURL = pub.getUrl();
			if (StringUtils.isNotEmpty(messageKey)) {
				pubURL = pubURL + "?key=" + messageKey;
			}
			PubResult pubResult = new PubResult(0, null);
			HttpPost httpPost = new HttpPost(pubURL);
			httpPost.setHeader(Constants.APPID, pub.getAppId());
			httpPost.setHeader(Constants.PUBKEY, pub.getKey());
			httpPost.setHeader(Constants.USERAGENT, Constants.USER_AGENT);
			String result = null;
			try {
				httpPost.setEntity(new StringEntity(message,Constants.CHARSET));

				HttpResponse response = httpClient.execute(httpPost);
				int statusCode = 0;
				if (response.getStatusLine() != null) {
					statusCode = response.getStatusLine().getStatusCode();
					pubResult.setStatusCode(statusCode);
					Header offsetHeader = response.getFirstHeader(Constants.XOFFSET);
					if (offsetHeader != null) {
						pubResult.setOffset(NumberUtils.parseLong(offsetHeader.getValue()));
					}
					Header partitionHeader = response.getFirstHeader(Constants.XPartition);
					if (partitionHeader != null) {
						pubResult.setPartition(NumberUtils.parseInt(partitionHeader.getValue()));
					}
				} else {
					logger.error("StatusLine is null ");
				}

				if (statusCode != 201) {
					HttpEntity entity = response.getEntity();
					if (entity != null) {
						result = EntityUtils.toString(entity, "UTF-8");
						JSONObject jsonObject = JSONObject.parseObject(result);
						pubResult.setMessage(jsonObject.getString("errmsg"));
						// 状态码为4XX抛出异常
						if (NumberUtils.judge4xx(statusCode)) {
							logger.warn(String.format("AuthorityErrorException thrown after got: %s", jsonObject.getString("errmsg")));
							throw new AuthorityErrorException(statusCode, jsonObject.getString("errmsg"));
						}
						httpPost.abort();
					}
				}
				EntityUtils.consume(response.getEntity());
			} catch (UnsupportedEncodingException e) {
				pubResult.setMessage("消息发送异常:" + e.getMessage());
				logger.error("send msg error:", e);
			} catch (ClientProtocolException e) {
				pubResult.setMessage("消息发送异常:" + e.getMessage());
				logger.error("send msg error:", e);
			} catch (IOException e) {
				pubResult.setMessage("消息发送异常:" + e.getMessage());
				logger.error("send msg error:", e);
			} catch (JSONException e) {
				logger.error(StatusCodeEnum.DATA_ERROR.getMessage() + "|" + result, e);
				pubResult.setStatusCode(StatusCodeEnum.DATA_ERROR.getStatus());
				pubResult.setMessage(StatusCodeEnum.DATA_ERROR.getMessage());
			} finally {
				if (httpPost != null) {
					httpPost.abort();
				}
			}
			return pubResult;
		}
}
