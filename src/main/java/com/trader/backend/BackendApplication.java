package com.trader.backend;

import com.trader.backend.service.LiveFeedService;
import com.trader.backend.service.NseInstrumentService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import javax.annotation.PostConstruct;

@SpringBootApplication
@EnableScheduling
public class BackendApplication {

	@Autowired
	private LiveFeedService liveFeedService;

	public static void main(String[] args) {
		SpringApplication.run(BackendApplication.class, args);
	}

}
