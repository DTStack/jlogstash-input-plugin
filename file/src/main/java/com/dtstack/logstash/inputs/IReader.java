package com.dtstack.logstash.inputs;

import java.io.IOException;

/**
 * 文件读取接口
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年11月25日
 * Company: www.dtstack.com
 * @author xuchao
 *
 */
public interface IReader {
	
	/**
	 * 读取文件一行,返回null表示读文件结束
	 * @return
	 * @throws IOException
	 * @throws Exception 
	 */
	public String readLine() throws Exception;
	
	/**
	 * 获取当前读文件名
	 * @return
	 */
	public String getFileName();
	
	/**
	 * 获取当前文件读取位置(byte)
	 * @return
	 */
	public long getCurrBufPos();
	
	/***
	 * 是否需监控文件内容变化
	 * @return
	 */
	public boolean needMonitorChg();
	
	/**
	 * 文件读取完需要处理的内容
	 * @throws IOException
	 */
	public void doAfterReaderOver() throws IOException;

}
