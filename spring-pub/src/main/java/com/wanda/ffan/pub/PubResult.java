package com.wanda.ffan.pub;

/**
 * 发送结果
 * <p/>
 * Created by liuzhenfeng on 16/3/16.
 */
public class PubResult {

    /**
     * 发送结果状态码 201表示发送成功，其它均为发送失败
     */
    private int statusCode;
    /**
     * kafka的partition ID, 当statusCode＝201时该值才有意义
     */
    private int partition;
    /**
     * 消息的offset, 当statusCode＝201时该值才有意义
     */
    private long offset;
    /**
     * 当statusCode不等于201时,返回错误信息
     */
    private String message;

    public PubResult(int statusCode, String message) {
        this.statusCode = statusCode;
        this.message = message;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }
}
