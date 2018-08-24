package org.shersfy.datahub.fs.executor.service;

import java.io.IOException;

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
import org.apache.hadoop.io.Text;
import org.shersfy.datahub.commons.connector.hadoop.HdfsUtil;
import org.shersfy.datahub.commons.meta.HdfsMeta;
import org.shersfy.datahub.fs.protocols.FsStreamService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSON;

@Component
public class FsStreamServiceImpl implements FsStreamService{
    
    Logger logger = LoggerFactory.getLogger(getClass());
    
    private FileSystem fs;
    
    private FSDataOutputStream output;
    
    private FSDataInputStream input;
    
    @Override
    public BooleanWritable connect(Text fsMeta) {
        
        BooleanWritable res = new BooleanWritable(true);
        try {

            if(fsMeta==null || StringUtils.isBlank(fsMeta.toString())) {
                fs = FileSystem.get(new Configuration(false));
            }
            else {
                HdfsMeta meta = JSON.parseObject(fsMeta.toString(), HdfsMeta.class);
                fs = HdfsUtil.getFileSystem(meta);
            }
            
        } catch (Exception e) {
            logger.error("", e);
            res.set(false);
        }
        
        return res;
    }

    @Override
    public BooleanWritable createNewFile(Text path) {
        BooleanWritable res = new BooleanWritable(true);
        try {
            output = fs.create(new Path(path.toString()), true);
        } catch (IllegalArgumentException | IOException e) {
            logger.error("", e);
            res.set(false);
        }
        return res;
    }

    @Override
    public BooleanWritable append(Text path) {
        BooleanWritable res = new BooleanWritable(true);
        try {
            output = fs.append(new Path(path.toString()), 1024);
        } catch (IllegalArgumentException | IOException e) {
            logger.error("", e);
            res.set(false);
        }
        return res;
    }

    @Override
    public BooleanWritable open(Text path) {
        BooleanWritable res = new BooleanWritable(true);
        try {
            input = fs.open(new Path(path.toString()));
        } catch (IllegalArgumentException | IOException e) {
            logger.error("", e);
            res.set(false);
        }
        return res;
    }
    
    @Override
    public void write(BytesWritable bytes) {
        
        try {
            output.write(bytes.getBytes(), 0, bytes.getLength());
            output.flush();
        } catch (Exception e) {
            logger.error("", e);
        }
        
    }

    @Override
    public IntWritable read(BytesWritable bytes) {
        IntWritable res = new IntWritable(0);
        try {
            res.set(input.read(bytes.getBytes()));
        } catch (IllegalArgumentException | IOException e) {
            logger.error("", e);
        }
        return res;
    }

    @Override
    public void closeInputStream() {
        IOUtils.closeQuietly(input);
    }
    
    @Override
    public void closeOutputStream() {
        IOUtils.closeQuietly(output);
    }


}
