package com.dtstack.logstash.inputs;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 暂时只支持.rar,.zip,.tar.gz,.tar,普通文本
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年11月22日
 * Company: www.dtstack.com
 * @author xuchao
 *
 */
public class ReadFactory {
	
	public static IReader createReader(java.io.File file, String encoding, ConcurrentHashMap<String, Long> fileCurrPos
			, String startPos) throws IOException{
		
		IReader reader = null;
		String fileName = file.getPath();
		if(fileName.toLowerCase().endsWith(".zip")){
			reader = ReadZipFile.createInstance(fileName, encoding, fileCurrPos);
		}else if(fileName.toLowerCase().endsWith(".rar")){
			reader = ReadRarFile.createInstance(fileName, encoding, fileCurrPos);
		}else if(fileName.toLowerCase().endsWith(".tar.gz") || fileName.toLowerCase().endsWith(".tar")){
			reader = ReadTarFile.createInstance(fileName, encoding, fileCurrPos);
		}else{
			Long filePos = fileCurrPos.get(fileName);
			
			if(filePos == null){//未读取过的根据配置的读取点开始
				reader = new ReadLineUtil(file, encoding, startPos);
			}else{
				reader = new ReadLineUtil(file, encoding, filePos);
			}
		}
		
		return reader;
	}
	
	

}
