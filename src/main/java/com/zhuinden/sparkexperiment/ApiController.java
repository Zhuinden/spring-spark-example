package com.zhuinden.sparkexperiment;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;

/**
 * Created by Owner on 2017. 03. 29..
 */
@RequestMapping("api")
@Controller
public class ApiController {
    @Autowired
    WordCount wordCount;

    @Autowired
    MongoDB mongo;
    
    @Autowired
    Mysql mysql;
    
    @RequestMapping("wordcount")
    public ResponseEntity<List<Count>> words() {
        return new ResponseEntity<>(wordCount.count(), HttpStatus.OK);
    }
    

    @RequestMapping(value="mongodb",method = RequestMethod.POST) 
    public ResponseEntity<Long> MongoDB(
    		@RequestParam(value="Database") String Database,
    		@RequestParam(value="Collection") String Collection
    		
    		) {
    	
    	 	long responsce = mongo.count(Database, Collection);
    	 	
    	return new ResponseEntity<Long>(responsce, HttpStatus.OK);
    	  
    }
    
    @RequestMapping(value="mysql",method = RequestMethod.POST) 
    public ResponseEntity<List<String>> MYSQl(
    		@RequestParam(value="JDBC_URI") String JDBC_URI,
    		@RequestParam(value="JDBC_username") String JDBC_username,
    		@RequestParam(value="JDBC_password") String JDBC_password
    		
    		) {
    	
    	List<String> responsce = mysql.getTableNames(JDBC_URI, JDBC_username, JDBC_password);
    	 	
    	 	
    	return new ResponseEntity<List<String>>(responsce, HttpStatus.OK);
    	  
    }
    
    
}
