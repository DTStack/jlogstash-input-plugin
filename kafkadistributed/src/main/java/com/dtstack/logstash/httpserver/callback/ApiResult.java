package com.dtstack.logstash.httpserver.callback;

/**
 * 
 * @author sishu.yss
 *
 */
public class ApiResult {

	private int code;
	private String msg;
	private String errorMsg;
	private long space;
	private String requestId;

	public ApiResult(){
		setRequestId(RequestContext.get().getRequestId());
	}
	/**
	 * @return the requestId
	 */
	public String getRequestId() {
		return requestId;
	}


	/**
	 * @param requestId the requestId to set
	 */
	public void setRequestId(String requestId) {
		this.requestId = requestId;
	}


	public long getSpace() {
		return space;
	}


	public void setSpace(long space) {
		this.space = space;
	}


	public String getErrorMsg() {
		return errorMsg;
	}


	public void setErrorMsg(String errorMsg) {
		this.errorMsg = errorMsg;
	}

	private Object data;

	/**
	 * @return the code
	 */
	public int getCode() {
		return code;
	}


	/**
	 * @param code
	 *            the code to set
	 */
	public void setCode(int code) {
		this.code = code;
	}

	/**
	 * @return the msg
	 */
	public String getMsg() {
		return msg;
	}

	/**
	 * @param msg
	 *            the msg to set
	 */
	public void setMsg(String msg) {
		this.msg = msg;
	}


	public void serverError() {
		this.setCodeMsg(500, "Internal Server Error");
	}

	public void setCodeMsg(int code, String msg) {
		this.setCode(code);
		this.setMsg(msg);
	}
	
	public void success(Object data){
		this.setCodeMsg(200, "OK");
		this.setData(data);
	}
	

	public Object getData() {
		return data;
	}


	public void setData(Object data) {
		this.data = data;
	}


	public void noContent(){
		this.setCodeMsg(204, "No Content");
	}
	
	public void notModified(){
		this.setCodeMsg(304, "Not Modified");
	}

}
