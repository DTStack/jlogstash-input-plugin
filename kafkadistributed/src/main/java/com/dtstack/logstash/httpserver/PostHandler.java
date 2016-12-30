package com.dtstack.logstash.httpserver;


import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import com.sun.net.httpserver.HttpExchange;
import  com.sun.net.httpserver.HttpHandler;


/**
 * 
 * @author sishu.yss
 *
 */
@SuppressWarnings("restriction")
public class PostHandler implements HttpHandler{

	@Override
	public void handle(HttpExchange he) throws IOException {
		// TODO Auto-generated method stub
        OutputStream os = he.getResponseBody();
        os.close();
	}
	
	
	@SuppressWarnings("unchecked")
	protected static void parseQuery(String query, Map<String, 
			Object> parameters) throws UnsupportedEncodingException {
		         if (query != null) {
		                 String pairs[] = query.split("[&]");
		                 for (String pair : pairs) {
		                          String param[] = pair.split("[=]");
		                          String key = null;
		                          String value = null;
		                          if (param.length > 0) {
		                          key = URLDecoder.decode(param[0], 
		                          	System.getProperty("file.encoding"));
		                          }

		                          if (param.length > 1) {
		                                   value = URLDecoder.decode(param[1], 
		                                   System.getProperty("file.encoding"));
		                          }

		                          if (parameters.containsKey(key)) {
		                                   Object obj = parameters.get(key);
		                                   if (obj instanceof List<?>) {
		                                            List<String> values = (List<String>) obj;
		                                            values.add(value);

		                                   } else if (obj instanceof String) {
		                                            List<String> values = new ArrayList<String>();
		                                            values.add((String) obj);
		                                            values.add(value);
		                                            parameters.put(key, values);
		                                   }
		                          } else {
		                                   parameters.put(key, value);
		                          }
		                 }
		         }
		}

}
