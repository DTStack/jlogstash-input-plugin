package com.dtstack.logstash.httpserver.callback;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author sishu.yss
 *
 */
public class ApiCallbackMethod {
	private final static Logger logger = LoggerFactory
			.getLogger(ApiCallbackMethod.class);

	public static ApiResult doCallback(ApiCallback ac, String debug) {
		ApiResult apiResult = new ApiResult();
		try {
			long start = System.currentTimeMillis();
			ac.execute(apiResult);
			apiResult.setCode(200);
			long end = System.currentTimeMillis();
			apiResult.setSpace(end - start);
		} catch (Throwable re) {
		}
		return apiResult;
	}

}
