package org.shersfy.datahub.fs.executor.service;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;

public class FSDataStreamer {
    
    private FileSystem fs;
    
    private FSDataOutputStream output;
    
    private FSDataInputStream input;
    
    public FSDataStreamer(FileSystem fs) {
        super();
        this.fs = fs;
    }
    
    public FileSystem getFs() {
        return fs;
    }

    public void setFs(FileSystem fs) {
        this.fs = fs;
    }

    public FSDataOutputStream getOutput() {
        return output;
    }

    public void setOutput(FSDataOutputStream output) {
        this.output = output;
    }

    public FSDataInputStream getInput() {
        return input;
    }

    public void setInput(FSDataInputStream input) {
        this.input = input;
    }

}
