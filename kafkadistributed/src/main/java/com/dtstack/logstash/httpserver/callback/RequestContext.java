package com.dtstack.logstash.httpserver.callback;



/**
 * 
 * @author sishu.yss
 *
 */
public class RequestContext {

	private String requestId;
	
	private static ThreadLocal<RequestContext> context = new ThreadLocal<RequestContext>() {
		protected synchronized RequestContext initialValue() {
			return new RequestContext();
		}
	};

	public static RequestContext get() {
		return context.get();
	}

	public static void clearContext() {
		get().clear();
	}

	private void clear() {
		this.requestId = null;
	}

	/**
	 * @return the requestId
	 */
	public String getRequestId() {
		return requestId;
	}

	/**
	 * @param requestId
	 *            the requestId to set
	 */
	public void setRequestId(String requestId) {
		this.requestId = requestId;
	}
}
