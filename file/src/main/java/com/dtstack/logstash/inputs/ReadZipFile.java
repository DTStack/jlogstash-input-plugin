package com.dtstack.logstash.inputs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadZipFile implements IReader{
	
	private static final Logger logger = LoggerFactory.getLogger(ReadZipFile.class);
	
	private ZipInputStream zipIn;
	
	private ZipFile zfile;
	
	private BufferedReader currBuff;
	
	private InputStream currIns;
	
	private int currFileSize = 0;
	
	private String currFileName = null;
	
	private String encoding = "UTF-8"; 
	
	private ConcurrentHashMap<String, Integer> fileCurrPos; 
	
	private String zipFileName;
	
	public boolean readEnd = false;
	
	public static ReadZipFile createInstance(String fileName, String encoding, ConcurrentHashMap<String, Integer> fileCurrPos){
		ReadZipFile readZip = new ReadZipFile(fileName, encoding, fileCurrPos);
		if(readZip.init()){
			return readZip;
		}
		
		return null;
	}
	
	private ReadZipFile(String fileName, String encoding, ConcurrentHashMap<String, Integer> fileCurrPos){
		this.fileCurrPos = fileCurrPos;
		this.zipFileName = fileName;
		this.encoding = encoding;
	}
	
	private boolean init(){
		if (!zipFileName.toLowerCase().endsWith(".zip")) {
			logger.error("file:{} not zip file", zipFileName);
			return false;
		}

		File file = new File(zipFileName);

		try {
			zipIn = new ZipInputStream(new FileInputStream(file));
		} catch (FileNotFoundException e) {
			logger.error("", e);
			return false;
		}

		zfile = null;

		try {
			zfile = new ZipFile(zipFileName);
			getNextBuffer();
		} catch (Exception e) {
			logger.error("", e);
			return false;
		}
		
		return true;
	}
	
	private int getSkipNum(String identify){
		Integer skipNum = fileCurrPos.get(identify);
		skipNum = skipNum == null ? 0 : skipNum;
		return skipNum;
	}
	
	private String getIdentify(String fileName){
		return zipFileName + "|" + fileName; 
	}
	
	public BufferedReader getNextBuffer(){
		
		ZipEntry zipEn = null;
		if(currBuff != null){
			try {
				zipIn.closeEntry();
				currBuff.close();
			} catch (IOException e) {
				logger.error("", e);
			}
			currBuff = null;
		}
		
		if(currIns != null){
			try {
				currIns.close();
			} catch (IOException e) {
				logger.error("", e);
			}
			currIns = null;
			
			flushPos();
		}
		currFileName = null;
		currFileSize = 0;
		
		try {
			while ((zipEn = zipIn.getNextEntry()) != null) {
				if (!zipEn.isDirectory()) {					
					currIns = zfile.getInputStream(zipEn);
					String fileName = zipEn.getName();
					Integer skipNum =  getSkipNum(getIdentify(fileName));
					if(skipNum >= zipEn.getSize()){
						zipIn.closeEntry();
						continue;//跳过
					}
					
					currFileSize = (int) zipEn.getSize();
					currFileName = fileName;
					currIns.skip(skipNum);
					currBuff = new BufferedReader(new InputStreamReader(currIns, encoding));
					break;
				}
			}
		} catch (Exception e) {
			logger.error("", e);
		}
		
		if(currBuff == null){//释放资源
			try {
				zipIn.closeEntry();
				zfile.close();
				doAfterReaderOver();
				logger.warn("release file resourse..");
			} catch (IOException e) {
				logger.error("", e);
			};
		}
		
		return currBuff;
	}
	
	/**
	 * 清理之前记录的zip文件里的文件信息eg: e:\\d\xx.zip|mydata/aa.log
	 */
	private void doAfterReaderOver(){
		Iterator<Entry<String, Integer>> it = fileCurrPos.entrySet().iterator();
		String preFix = zipFileName + "|";
		while(it.hasNext()){
			Entry<String, Integer> entry = it.next();
			if(entry.getKey().startsWith(preFix)){
				it.remove();
			}
		}
		
		//重新插入一条表示zip包读取完成的信息
		fileCurrPos.put(zipFileName, -1);
		readEnd = true;
	}
		
	public String readLine() throws IOException{
		
		if(currBuff == null){
			return null;
		}
		
		String str = currBuff.readLine();
		if(str == null){
			BufferedReader buff = getNextBuffer();
			if(buff != null){
				str = currBuff.readLine();
			}
		}
		return str;
	}

	@Override
	public String getFileName() {
		if(currFileName == null){
			return zipFileName;
		}
		return getIdentify(currFileName);
	}

	@Override
	public int getCurrBufPos() {
		
		if(readEnd){
			return -1;
		}
		
		int available = 0;
		try {
			available = currIns.available();
		} catch (IOException e) {
			logger.error("error:", e);
			return 0;
		}
		
		return currFileSize - available;
	}
	
	/**
	 * 每个子文件写完的时候都更新一次
	 */
	public void flushPos(){
		
		if(currFileName == null){
			logger.error("invalid state, may have set currFileName null at not right place.");
			return;
		}
		
		fileCurrPos.put(getIdentify(currFileName), currFileSize);
	}
	
	public static void main(String[] args) throws IOException {
		ConcurrentHashMap<String, Integer> map = new ConcurrentHashMap<String, Integer>();
		ReadZipFile zipFile = new ReadZipFile("E:\\data\\mydata.zip", "utf-8", map);
		//zipFile.fileCurrPos.put(zipFile.getIdentify("mydata/log4.log"), 3);
		String str = null;
		while( (str = zipFile.readLine()) != null){
			System.out.println(str);
		}
		
		System.out.println();
		System.out.println(zipFile.fileCurrPos);
		
	}
}
