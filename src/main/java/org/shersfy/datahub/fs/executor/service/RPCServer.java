package org.shersfy.datahub.fs.executor.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.shersfy.datahub.fs.protocols.FsStreamService;
import org.shersfy.datahub.fs.protocols.StandardService;
import org.shersfy.datahub.fsexecutor.config.RPCServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class RPCServer {
    
    Logger logger = LoggerFactory.getLogger(getClass());
    
    @Autowired
    private RPCServerConfig config;
    
    @Autowired
    private StandardService standardService;
    
    @Autowired
    private FsStreamService fsStreamService;
    
    protected void init() {
        try {
            // StandardService
            new RPC.Builder(new Configuration(false))
            .setBindAddress(config.getHost())
            .setPort(config.getPort())
            .setProtocol(StandardService.class)
            .setInstance(standardService)
            .build()
            .start();
            
            // FsStreamService
            new RPC.Builder(new Configuration(false))
            .setBindAddress(config.getFsServiceHost())
            .setPort(config.getFsServicePort())
            .setProtocol(FsStreamService.class)
            .setInstance(fsStreamService)
            .build()
            .start();
        } catch (Exception e) {
            logger.error("", e);
        }
    }

}
