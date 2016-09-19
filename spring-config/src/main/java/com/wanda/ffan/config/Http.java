package com.wanda.ffan.config;
/**
 * 
 * @author zhangling
 *
 */
public class Http {
	private String id;
	    private int maxCon = 10;
	    private int rtimeOut = 2000;
	    private int ctimeOut = 2000;
	    private int stimeOut = 2000;
	    private int idleClose=3 * 60 * 1000;
		public int getMaxCon() {
			return maxCon;
		}
		public void setMaxCon(int maxCon) {
			this.maxCon = maxCon;
		}
		public int getRtimeOut() {
			return rtimeOut;
		}
		public void setRtimeOut(int rtimeOut) {
			this.rtimeOut = rtimeOut;
		}
		public int getCtimeOut() {
			return ctimeOut;
		}
		public void setCtimeOut(int ctimeOut) {
			this.ctimeOut = ctimeOut;
		}
		public int getStimeOut() {
			return stimeOut;
		}
		public void setStimeOut(int stimeOut) {
			this.stimeOut = stimeOut;
		}
		public int getIdleClose() {
			return idleClose;
		}
		public void setIdleClose(int idleClose) {
			this.idleClose = idleClose;
		}
		public Http() {
			super();
			// TODO Auto-generated constructor stub
		}
		public String getId() {
			return id;
		}
		public void setId(String id) {
			this.id = id;
		}
	    
	    
	    

}
