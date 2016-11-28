package com.dtstack.logstash.input.fileinput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dtstack.logstash.assembly.InputQueueList;
import com.dtstack.logstash.inputs.File;
import com.dtstack.logstash.inputs.ReadLineUtil;

public class Test {
	
	public void test(){
		InputQueueList inputList = new InputQueueList();
		Map<String, String> config = new HashMap<String, String>();
		config.put("codec", "plain");
		List<String> path = new ArrayList<String>();
		path.add("E:\\controller.log");
		path.add("E:\\server.log");
		
		File file = new File(config, inputList);
//		File.path = path;
		file.prepare();
		file.emit();
	}
	
	public void testReadLine() throws IOException{
		java.io.File file = new java.io.File("D:\\testReadLine.txt");
		ReadLineUtil readLineUtil = new ReadLineUtil(file, "UTF-8", 0);
		String line = null;
		while((line = readLineUtil.readLine()) != null){
			System.out.println(line);
		}
		
		System.out.println(readLineUtil.getCurrBufPos());
		
	}
	
	public static void main(String[] args) throws IOException {
		Test test = new Test();
		test.testReadLine();
	}

}
