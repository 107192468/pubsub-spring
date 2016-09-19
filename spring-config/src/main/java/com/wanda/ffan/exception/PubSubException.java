package com.wanda.ffan.exception;

public class PubSubException extends RuntimeException{
	 private static final long serialVersionUID = 1L;
	 public PubSubException(String message, Throwable cause) {
	        super(message, cause);
	    }

	    public PubSubException(String message) {
	        super(message);
	    }

	    public PubSubException(Throwable cause) {
	        super(cause);
	    }

	    public PubSubException() {
	        super();
	    }
}
