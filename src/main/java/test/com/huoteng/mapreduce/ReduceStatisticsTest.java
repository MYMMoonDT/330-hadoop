package test.com.huoteng.mapreduce; 

import com.huoteng.mapreduce.ReduceStatistics;
import org.junit.Test;
import org.junit.Before; 
import org.junit.After; 

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
//TODO: Test goes here... 
/* 
try { 
   Method method = ReduceStatistics.getClass().getMethod("rad", double.class); 
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
