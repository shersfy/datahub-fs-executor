package org.shersfy.datahub.fs.executor.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix="rpc.server")
public class RPCServerConfig {
    
    /**rpc server普通服务host, 默认本机IP*/
    private String host = "localhost";
    
    /**rpc server普通服务端口, 默认8899 */
    private int port = 8899;
    
    /**rpc server FileSystem服务host, 默认本机IP*/
    private String fsServiceHost = "localhost";
    
    /**rpc server FileSystem服务端口, 默认8877 */
    private int fsServicePort = 8877;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getFsServiceHost() {
        return fsServiceHost;
    }

    public void setFsServiceHost(String fsServiceHost) {
        this.fsServiceHost = fsServiceHost;
    }

    public int getFsServicePort() {
        return fsServicePort;
    }

    public void setFsServicePort(int fsServicePort) {
        this.fsServicePort = fsServicePort;
    }

    
}
