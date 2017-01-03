/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.logstash.http.server.callback;

/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年12月30日 下午1:16:37
 * Company: www.dtstack.com
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
