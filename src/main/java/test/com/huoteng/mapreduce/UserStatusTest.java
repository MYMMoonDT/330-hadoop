package test.com.huoteng.mapreduce; 

import com.huoteng.mapreduce.UserStatus;
import org.junit.Test;
import org.junit.Before; 
import org.junit.After; 

/** 
* UserStatus Tester. 
* 
* @author <Authors name> 
* @since <pre>Nov 24, 2015</pre> 
* @version 1.0 
*/ 
public class UserStatusTest { 

@Before
public void before() throws Exception { 
} 

@After
public void after() throws Exception { 
} 

/** 
* 
* Method: judgeUserDate(String dateString) 
* 
*/ 
@Test
public void testJudgeUserDate() throws Exception { 
    System.out.println("TIME:" + UserStatus.getUserTime("2015-04-08 12:18:00.541373"));
    System.out.println("DATE:" + UserStatus.judgeUserDate("2015-04-30 12:18:00.541373"));
} 

/** 
* 
* Method: getUserTime(String dateString) 
* 
*/ 
@Test
public void testGetUserTime() throws Exception { 
//TODO: Test goes here... 
} 


} 
