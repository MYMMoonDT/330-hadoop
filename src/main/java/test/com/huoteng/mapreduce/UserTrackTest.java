package test.com.huoteng.mapreduce; 

import com.huoteng.mapreduce.Coordinate;
import com.huoteng.mapreduce.UserTrack;
import org.junit.Test;
import org.junit.Before; 
import org.junit.After;

import java.util.HashSet;
import java.util.Set;

/** 
* UserTrack Tester. 
* 
* @author <Authors name> 
* @since <pre>Nov 5, 2015</pre> 
* @version 1.0 
*/ 
public class UserTrackTest { 

@Before
public void before() throws Exception { 
} 

@After
public void after() throws Exception { 
} 

/** 
* 
* Method: main(String[] args) 
* 
*/ 
@Test
public void testMain() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) 
* 
*/ 
@Test
public void testMap() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) 
* 
*/ 
@Test
public void testReduce() throws Exception { 
//TODO: Test goes here... 
} 


/** 
* 
* Method: judgeUserStatus(String dateString) 
* 
*/ 
@Test
public void testJudgeUserStatus() throws Exception { 
//TODO: Test goes here... 
/* 
try { 
   Method method = UserTrack.getClass().getMethod("judgeUserStatus", String.class); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/

   System.out.println(UserTrack.Map.judgeUserStatus("2015-11-02 00:14:00.455487"));
   System.out.println(UserTrack.Map.judgeUserStatus("2015-11-01 00:14:00.455487"));
   System.out.println(UserTrack.Map.judgeUserStatus("2015-11-01 11:14:00.455487"));
   System.out.println(UserTrack.Map.judgeUserStatus("2015-11-02 11:14:00.455487"));
   System.out.println(UserTrack.Map.judgeUserStatus("2015-11-01 00:14:00.455487"));
   System.out.println(UserTrack.Map.judgeUserStatus("2015-11-01 11:14:00.455487"));
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
   Method method = UserTrack.getClass().getMethod("twoPointIsClose", Coordinate.class, Coordinate.class); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/

//   System.out.println(UserTrack.Reduce.twoPointIsClose(new Coordinate("28.688600", "115.904400"), new Coordinate("28.680601", "115.914410")));
//   System.out.println(UserTrack.Reduce.twoPointIsClose(new Coordinate("28.688600", "115.904400"), new Coordinate("28.546800", "116.095100")));
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
   Method method = UserTrack.getClass().getMethod("GetDistance", double.class, double.class, double.class, double.class); 
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
   Method method = UserTrack.getClass().getMethod("rad", double.class); 
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
* Method: findPlace(Set<Coordinate> coordinateSet) 
* 
*/ 
@Test
public void testFindPlace() throws Exception { 
//TODO: Test goes here... 
/* 
try { 
   Method method = UserTrack.getClass().getMethod("findPlace", Set<Coordinate>.class); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/

//   final Coordinate test1 = new Coordinate("115.839400|28.654400", "");
//   final Coordinate test2 = new Coordinate("115.839400|28.654400", "");
//   final Coordinate test3 = new Coordinate("115.839400|28.654400", "");
//   test1.sum = 1;
//   test2.sum = 2;
//   test3.sum = 1;
//
//   Set<Coordinate> coordinates = new HashSet<Coordinate>(){{
//      add(test1);
//      add(test2);
//      add(test3);
//   }};
//
//
//   Coordinate target = UserTrack.Reduce.findPlace(coordinates);
//   System.out.println(target.sum);
} 

} 
