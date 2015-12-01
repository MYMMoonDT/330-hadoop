package test.com.huoteng.mapreduce; 

import org.junit.Test;
import org.junit.Before; 
import org.junit.After;

import java.io.*;

/** 
* ReduceStatistics Tester. 
* 
* @author <Authors name> 
* @since <pre>Nov 9, 2015</pre> 
* @version 1.0 
*/ 
public class ReduceStatisticsTest { 

@Before
public void before() throws Exception { 
} 

@After
public void after() throws Exception { 
} 

/** 
* 
* Method: statisticsTrack(String recordsStr) 
* 
*/ 
@Test
public void testStatisticsTrack() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: getResult() 
* 
*/ 
@Test
public void testGetResult() throws Exception { 
//TODO: Test goes here... 
} 


/** 
* 
* Method: twoPointIsClose(Coordinate c1, Coordinate c2) 
* 
*/ 
@Test
public void testTwoPointIsClose() throws Exception { 
//TODO: Test goes here... 
/* 
try { 
   Method method = ReduceStatistics.getClass().getMethod("twoPointIsClose", Coordinate.class, Coordinate.class); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/ 
} 

/** 
* 
* Method: GetDistance(double lng1, double lat1, double lng2, double lat2) 
* 
*/ 
@Test
public void testGetDistance() throws Exception { 
//TODO: Test goes here... 
/* 
try { 
   Method method = ReduceStatistics.getClass().getMethod("GetDistance", double.class, double.class, double.class, double.class); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/ 
} 

/** 
* 
* Method: rad(double d) 
* 
*/ 
@Test
public void testRad() throws Exception {
   File inputFile = new File("/Users/huoteng/Desktop/output/part-00000_local");

   InputStream input = new FileInputStream(inputFile);
   BufferedReader reader = new BufferedReader(new InputStreamReader(input));

   String line = reader.readLine();
   System.out.println("LINE:" + line);


   String[] result = line.split("\\t");
   System.out.println("A:" + result[0]);
   System.out.println("B:" + result[1]);

} 

   /**
   *
   * Method: sortCoordinates(ArrayList<Coordinate> coordinateArray)
   *
   */
   @Test
   public void testSortCoordinates() throws Exception {
   //TODO: Test goes here...
      StringBuffer test = new StringBuffer("wertyiopgfdfvbnm,mnbvcjkkjashdfjelhrhjkadhfgklhweirlhl;");
      test.delete(test.length()-1, test.length());
      System.out.println(test);
   }

} 
