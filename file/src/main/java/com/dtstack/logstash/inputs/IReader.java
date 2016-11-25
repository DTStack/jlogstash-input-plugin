package com.dtstack.logstash.inputs;

import java.io.IOException;

public interface IReader {
	
	public String readLine() throws IOException;
	
	public String getFileName();
	
	public int getCurrBufPos();

}
