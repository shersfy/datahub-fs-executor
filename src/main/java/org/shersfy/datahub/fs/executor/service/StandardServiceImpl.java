package org.shersfy.datahub.fs.executor.service;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.shersfy.datahub.commons.connector.hadoop.HdfsUtil;
import org.shersfy.datahub.commons.meta.HdfsMeta;
import org.shersfy.datahub.fs.protocols.StandardService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class StandardServiceImpl implements StandardService {
    
    private Map<String, FSDataStreamer> cache = new ConcurrentHashMap<>();
    
    Logger logger = LoggerFactory.getLogger(getClass());
    

    @Override
    public void initClient(Text clientId) {
        if(clientId == null 
            || StringUtils.isBlank(clientId.toString())) {
            return;
        }
        
        
        for(String key :cache.keySet()) {
            if(!key.startsWith(clientId.toString())) {
                continue;
            }
            fsClose(new Text(key));
        }
        logger.info("clear history cache from client {}", clientId.toString());
    }

    @Override
    public Text fsConnect(Text clientId, LongWritable resId) {
        
        if(clientId == null) {
            return null;
        }
        
        // resId查询资源信息
        HdfsMeta meta = new HdfsMeta();
        meta.setUserName("hdfs");
        meta.setUrl("hdfs://192.168.186.129:9000/");
        
        String key = generateKey(clientId.toString());
        try {

            FileSystem fs = null;
            if(resId==null) {
                fs = FileSystem.get(new Configuration(false));
            }
            else {
                fs = HdfsUtil.getFileSystem(meta);
            }
            
            FSDataStreamer fsds = new FSDataStreamer(fs);
            cache.put(key, fsds);
            
        } catch (Exception e) {
            logger.error("", e);
            return null;
        }
        
        return new Text(key);
    }

    @Override
    public BooleanWritable fsCreateNewFile(Text key, Text path) {
        BooleanWritable res = new BooleanWritable(true);
        FSDataStreamer fsds = cache.get(key.toString());
        if(fsds==null) {
            res.set(false);
            return res;
        }
        
        try {
            String user = System.getProperty(HdfsUtil.HADOOP_USER_NAME);
            FSDataOutputStream output = HdfsUtil.createHdfsFile(fsds.getFs(), path.toString(), user);
            fsds.setOutput(output);
        } catch (Exception e) {
            logger.error("", e);
            res.set(false);
        }
        
        return res;
    }

    @Override
    public BooleanWritable fsAppend(Text key, Text path) {
        BooleanWritable res = new BooleanWritable(true);
        FSDataStreamer fsds = cache.get(key.toString());
        
        if(fsds==null) {
            res.set(false);
            return res;
        }
        
        try {
            FSDataOutputStream output = fsds.getFs().append(new Path(path.toString()), 1024);
            fsds.setOutput(output);
        } catch (IllegalArgumentException | IOException e) {
            logger.error("", e);
            res.set(false);
        }
        
        return res;
    }

    @Override
    public BooleanWritable fsOpen(Text key, Text path) {
        BooleanWritable res = new BooleanWritable(true);
        FSDataStreamer fsds = cache.get(key.toString());
        
        if(fsds==null) {
            res.set(false);
            return res;
        }
        
        try {
            FSDataInputStream input = fsds.getFs().open(new Path(path.toString()));
            fsds.setInput(input);
        } catch (IllegalArgumentException | IOException e) {
            logger.error("", e);
            res.set(false);
        }
        return res;
    }

    @Override
    public void fsWrite(Text key, BytesWritable bytes) {
        FSDataStreamer fsds = cache.get(key.toString());
        if(fsds==null) {
            return;
        }
        
        try {
            fsds.getOutput().write(bytes.getBytes(), 0, bytes.getLength());
            fsds.getOutput().flush();
        } catch (Exception e) {
            logger.error("", e);
        }
        
    }

    @Override
    public IntWritable fsRead(Text key, BytesWritable bytes) {
        IntWritable len = new IntWritable(-1);
        FSDataStreamer fsds = cache.get(key.toString());
        if(fsds==null) {
            return len;
        }
        
        try {
            len.set(fsds.getInput().read(bytes.getBytes()));
        } catch (IllegalArgumentException | IOException e) {
            logger.error("", e);
        }
        
        return len;
    }

    @Override
    public void fsClose(Text key) {
        if(key == null) {
            return;
        }
        
        FSDataStreamer fsds = cache.get(key.toString());
        if(fsds!=null) {
            IOUtils.closeQuietly(fsds.getInput());
            IOUtils.closeQuietly(fsds.getOutput());
            cache.remove(key.toString());
        }
        
    }
    
    private String generateKey(String clientId) {
        return String.format("%s_%s", clientId, System.nanoTime());
    }


}
