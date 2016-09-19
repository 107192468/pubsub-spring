package com.wanda.ffan.enums;

/**
 * 返回值
 * 
 * @author holdzhuu
 *
 */
public enum StatusCodeEnum {
	DATA_NULL(204, "数据为空"), 
	SEND_SUCCESS(201, "发送成功"),
	RECEIVE_SUCCESS(200, "接收成功"), 
	CONN_ERROR(205, "链接异常"), 
	PARAMS_ERROR(550, "参数异常"),
	DATA_ERROR(551, "数据格式异常");
	private int status;
	private String message;

	private StatusCodeEnum(int status, String message) {
		this.status = status;
		this.message = message;
	}

	public int getStatus() {
		return status;
	}

	public String getMessage() {
		return message;
	}

}
