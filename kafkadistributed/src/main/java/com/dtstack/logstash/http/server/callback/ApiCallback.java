package com.dtstack.logstash.http.server.callback;


/**
 * 
 * @author sishu.yss
 *
 */
public interface ApiCallback {

	void execute(ApiResult apiResult) throws Exception;
}
