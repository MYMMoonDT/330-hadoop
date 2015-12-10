package placeAnalyzer;

import com.huoteng.placeAnalyzer.Coordinate;
import com.huoteng.placeAnalyzer.UserStatus;
import org.junit.Test; 
import org.junit.Before; 
import org.junit.After;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** 
* Coordinate Tester. 
* 
* @author <Authors name> 
* @since <pre>Dec 10, 2015</pre> 
* @version 1.0 
*/ 
public class CoordinateTest { 

@Before
public void before() throws Exception { 
} 

@After
public void after() throws Exception { 
} 



    /**
    *
    * Method: compareTo(Coordinate temp)
    *
    */
    @Test
    public void testCompareTo() throws Exception {
//        List<Coordinate> testCoordinates = new ArrayList<Coordinate>();
//
//        int second = UserStatus.getUserTime("2015-04-07 22:18:00.541373");
//        testCoordinates.add(new Coordinate("28.654400", "115.839400", second));
//
//        second = UserStatus.getUserTime("2015-04-07 21:18:00.229876");
//        testCoordinates.add(new Coordinate("28.615400", "115.959500", second));
//
//        second = UserStatus.getUserTime("2015-04-07 20:18:00.229876");
//        testCoordinates.add(new Coordinate("28.615400", "115.959500", second));
//
//        second = UserStatus.getUserTime("2015-04-07 19:18:00.229876");
//        testCoordinates.add(new Coordinate("28.615400", "115.959500", second));
//
//        second = UserStatus.getUserTime("2015-04-07 04:18:00.229876");
//        testCoordinates.add(new Coordinate("28.615400", "115.959500", second));
//
//        second = UserStatus.getUserTime("2015-04-07 05:18:00.229876");
//        testCoordinates.add(new Coordinate("28.615400", "115.959500", second));
//
//        second = UserStatus.getUserTime("2015-04-07 06:18:00.229876");
//        testCoordinates.add(new Coordinate("28.615400", "115.959500", second));
//
//        second = UserStatus.getUserTime("2015-04-07 13:18:00.229876");
//        testCoordinates.add(new Coordinate("28.615400", "115.959500", second));
//
//        second = UserStatus.getUserTime("2015-04-07 08:18:00.229876");
//        testCoordinates.add(new Coordinate("28.615400", "115.959500", second));
//
//        second = UserStatus.getUserTime("2015-04-07 09:18:00.229876");
//        testCoordinates.add(new Coordinate("28.615400", "115.959500", second));
//
//        second = UserStatus.getUserTime("2015-04-07 00:18:00.229876");
//        testCoordinates.add(new Coordinate("28.615400", "115.959500", second));
//
//        for (Coordinate i : testCoordinates) {
//            System.out.println("TIME:" + i.time);
//        }
//
//        System.out.println();
//        Collections.sort(testCoordinates);
//
//        for (Coordinate i : testCoordinates) {
//            System.out.println("TIME:" + i.time);
//        }

        ArrayList<Integer> test = new ArrayList<Integer>();

        test.add(10);
        test.add(100);
        test.add(1);
        test.add(0);
        test.add(23);
        test.add(2);
        test.add(98);
        test.add(45);
        test.add(13);
        test.add(10);

        for (int i : test) {
            System.out.println(i);
        }
        System.out.println();
        Collections.sort(test);

        for (int i : test) {
            System.out.println(i);
        }
    }


} 
