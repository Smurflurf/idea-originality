package de.simon.originality;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync 
public class Application {
	public static void main(String[] args) {
		System.setProperty("java.awt.headless", "true");
		SpringApplication.run(Application.class, args);
	}
}
