package com.dtstack.logstash.inputs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:17:27
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
@SuppressWarnings("serial")
public class Stdin extends BaseInput {
    private static final Logger logger = LoggerFactory.getLogger(Stdin.class);

    public Stdin(Map<String, Object> config){
        super(config);
    }

    @Override
    public void prepare() {
    }

    public void emit() {
        try {
            BufferedReader br =
                    new BufferedReader(new InputStreamReader(System.in));

            String input;

            while ((input = br.readLine()) != null) {
                try {
                    Map<String, Object> event = this.decoder
                            .decode(input);
                    this.process(event);
                } catch (Exception e) {
                    logger.error("{}:process event failed:{}",input,e.getCause());
                }
            }
        } catch (IOException io) {
            logger.error("Stdin loop got exception:{}",io.getCause());
        }
    }

	@Override
	public void release() {
		// TODO Auto-generated method stub
		
	}
}
