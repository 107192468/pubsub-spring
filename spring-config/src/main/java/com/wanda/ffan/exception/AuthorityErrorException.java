package com.wanda.ffan.exception;

/**
 * 消息状态码为4XX的都抛此异常
 * 
 * @author holdzhuu
 *
 */
public class AuthorityErrorException extends RuntimeException {
	private int statusCode;
	/**
	 * 
	 */
	private static final long serialVersionUID = 3610248799009304460L;

	public AuthorityErrorException() {

	}

	public AuthorityErrorException(int statusCode, String msg) {
		super(msg);
		this.statusCode = statusCode;
	}

	public int getStatusCode() {

		return statusCode;
	}

	public void setStatusCode(int statusCode) {
		this.statusCode = statusCode;
	}

}
