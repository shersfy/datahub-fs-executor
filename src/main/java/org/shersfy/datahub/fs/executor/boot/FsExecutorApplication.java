package org.shersfy.datahub.fs.executor.boot;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@ComponentScan("org.shersfy.datahub.fs.executor")
@SpringBootApplication
public class FsExecutorApplication {

	public static void main(String[] args) {
	    
		SpringApplication.run(FsExecutorApplication.class, args);
	}

}
