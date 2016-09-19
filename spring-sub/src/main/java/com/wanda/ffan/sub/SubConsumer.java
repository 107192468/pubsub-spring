package com.wanda.ffan.sub;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.wanda.ffan.common.Constants;
import com.wanda.ffan.common.TopicPartition;
import com.wanda.ffan.common.utils.NumberUtils;
import com.wanda.ffan.config.Http;
import com.wanda.ffan.config.Sub;
import com.wanda.ffan.consumer.Consumer;
import com.wanda.ffan.consumer.ConsumerRecord;
import com.wanda.ffan.consumer.ConsumerRecords;
import com.wanda.ffan.enums.StatusCodeEnum;
import com.wanda.ffan.exception.AuthorityErrorException;


/**
 * Created by zhangling on 2016/9/16.
 */
public class SubConsumer implements Consumer {
    private static final Logger logger = LoggerFactory.getLogger(SubConsumer.class);
    private final Http defaultHttp;
    private final Sub defaultSub;
    private final CloseableHttpClient defaultHttpClient;
    private HttpGet httpGet;
    private HttpResponse response;
    public SubConsumer(Http defaultHttp, Sub defaultSub, CloseableHttpClient defaultHttpClient) {
        this.defaultHttp = defaultHttp;
        this.defaultSub = defaultSub;
        this.defaultHttpClient = defaultHttpClient;
        init();
    }

    private void init(){
    	 Assert.notNull(defaultHttp, "defaultHttp must not be null");
    	 Assert.notNull(defaultSub, "defaultSub must not be null");
    	 Assert.notNull(defaultHttpClient, "defaultHttpClient must not be null");
        httpGet = new HttpGet(defaultSub.getUrl());
		httpGet.setHeader("Appid", defaultSub.getAppId());
		httpGet.setHeader("Subkey", defaultSub.getKey());
		httpGet.setHeader("User-Agent", Constants.USER_AGENT);
    }
    @Override
    public Set<String> subscription() {
        return null;
    }

    @Override
    public void subscribe(Collection<String> topics) {

    }

    @Override
    public ConsumerRecords poll(long timeout) {

        Map<TopicPartition, List<ConsumerRecord>> records=new HashMap<TopicPartition, List<ConsumerRecord>>() ;
        
        List<ConsumerRecord> crs=new ArrayList<>();
        TopicPartition tp =null;
       
		try {
			response = defaultHttpClient.execute(httpGet);

			StatusLine statusLine = response.getStatusLine();
			int status = statusLine.getStatusCode();
			Header offsetHeader = response.getFirstHeader("X-Offset");
			long offset=-1l;
			if (offsetHeader != null) {
				offset=NumberUtils.parseLong(offsetHeader.getValue());
			}
			Header partitionHeader = response.getFirstHeader("X-Partition");
			int partition=-1;
			tp =new TopicPartition(defaultSub.getKey(),partition);
			if (partitionHeader != null) {
				partition=NumberUtils.parseInt(partitionHeader.getValue());
				//defaultSub.getKey() 这个地方用defaultSub.getKey()做topic主要是因为公司将sub封装的没地方可以填写massagekey，所以该字段为了防止重复直接用subkey
				tp =new TopicPartition(defaultSub.getKey(),partition);
			}
			HttpEntity entity = response.getEntity();
			if (entity != null) {
				String result = EntityUtils.toString(entity, Constants.CHARSET);
				if(logger.isDebugEnabled()){
				logger.debug("receive entity:" + result);
				}
				if (status != StatusCodeEnum.RECEIVE_SUCCESS.getStatus()) {
					JSONObject jsonObject = JSONObject.parseObject(result);
					// 状态码为4XX抛出异常
					if (NumberUtils.judge4xx(status)) {
						logger.warn(String.format("AuthorityErrorException thrown after got: %s", jsonObject.getString("errmsg")));
						throw new AuthorityErrorException(status, jsonObject.getString("errmsg"));
					}
					this.close();
				} else {
					 ConsumerRecord cr =new ConsumerRecord(defaultSub.getKey(),partition,offset,result,defaultSub.getKey(),System.currentTimeMillis());
					 crs.add(cr);
				}
				EntityUtils.consume(entity);
			} 
		} catch (ClientProtocolException e) {
			logger.error(StatusCodeEnum.CONN_ERROR.getMessage(), e);
		} catch (IOException e) {
			logger.error(StatusCodeEnum.CONN_ERROR.getMessage(), e);
		} catch (JSONException e) {
			logger.error(StatusCodeEnum.DATA_ERROR.getMessage(), e);
		} finally {
			httpGet.releaseConnection();
		}
        
        
		 records.put(tp,crs);
        
        
        return new ConsumerRecords(records);
    }

    @Override
    public void close() {
    	httpGet.abort();
    }

    @Override
    public void wakeup() {
    	//TODO
    }

    @Override
    public void pause(Collection<TopicPartition> partitions) {
    	//TODO
    }
}
