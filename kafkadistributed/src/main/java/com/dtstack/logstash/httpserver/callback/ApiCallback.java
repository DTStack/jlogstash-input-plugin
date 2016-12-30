package com.dtstack.logstash.httpserver.callback;


/**
 * 
 * @author sishu.yss
 *
 */
public interface ApiCallback {

	void execute(ApiResult apiResult) throws Exception;
}
