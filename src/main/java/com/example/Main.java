/*
 * Copyright 2002-2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.example.kafka.DemoProducer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

@RestController
@SpringBootApplication
public class Main {

	@Value("${spring.datasource.url}")
	private String dbUrl;

	@Autowired
	private DataSource dataSource;
	
	@Autowired
	private DemoProducer producer;

	public static void main(String[] args) throws Exception {
		SpringApplication.run(Main.class, args);
	}

	@RequestMapping("/")
	String index() {
		return "index";
	}
	
	@RequestMapping(value = "/ple")
	List<Ple> listPle(@RequestParam(value = "insee", defaultValue = "World") String insee) throws Exception {
		Connection connection = dataSource.getConnection();

		Statement stmt = connection.createStatement();

		ResultSet rs = stmt.executeQuery("select acc.* from salesforce.account acc where acc.Siren__c like '" + insee+"'");
		
		System.out.println("select acc.* from salesforce.account acc where acc.Siren__c like '" + insee+"'");
		ArrayList<Ple> output = new ArrayList<Ple>();
		while (rs.next()) {
			Ple ple = new Ple();
			ple.setInsee(rs.getString("Siren__c"));
			ple.setSfid(rs.getString("sfid"));
			output.add(ple);
		}

		return output;

	}
	
	@RequestMapping(value = "/ciblage")
	String ciblage(@RequestParam(value = "insee", defaultValue = "World") String insee) throws Exception {
		
		
		Connection connection = dataSource.getConnection();

		Statement stmt = connection.createStatement();

		ResultSet rs = stmt.executeQuery("select acc.* from salesforce.account acc where acc.Siren__c like '" + insee+"'");
		Date idxDate = new Date();
		System.out.println("select acc.* from salesforce.account acc where acc.Siren__c like '" + insee+"'");
		while (rs.next()) {
			Ple ple = new Ple();
			ple.setInsee(rs.getString("Siren__c"));
			ple.setSfid(rs.getString("sfid"));
			ple.setIdxDate(idxDate);
			ObjectMapper mapper = new ObjectMapper();
			
			producer.send(mapper.writeValueAsString(ple));
		}
		
	
		
		
		return "OK";
	}
	

	@Bean
	public DataSource dataSource() throws SQLException {
		if (dbUrl == null || dbUrl.isEmpty()) {
			return new HikariDataSource();
		} else {
			HikariConfig config = new HikariConfig();
			config.setJdbcUrl(dbUrl);
			return new HikariDataSource(config);
		}
	}

}
