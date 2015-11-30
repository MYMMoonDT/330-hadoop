package test.com.huoteng.mapreduce; 

import com.huoteng.mapreduce.Coordinate;
import com.huoteng.mapreduce.UserStatus;
import org.junit.Test;
import org.junit.Before; 
import org.junit.After;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
    System.out.println("TIME:" + UserStatus.getUserTime("2015-04-08 12:18:23"));
}

    /**
    *
    * Method: getUserTime(String dateString)
    *
    */
    @Test
    public void testGetUserTime() throws Exception {
        System.out.println("TIME:" + UserStatus.getUserTime("2015-04-08 12:18:23"));
    }

    @Test
    public void testJudgeTimeValid() throws Exception {
        System.out.println("VALID:" + UserStatus.judgeTimeValid("2015-04-05 11:00:00"));
    }

    @Test
    public void testGetHome_WorkTimePoint() throws Exception {
        List<Coordinate> testCoordinates = new ArrayList<Coordinate>();

        String secondString = UserStatus.getUserTime("2015-04-07 22:18:00.541373");
        int secondNum = Integer.parseInt(secondString);
        testCoordinates.add(new Coordinate("28.654400", "115.839400", secondNum));

        secondString = UserStatus.getUserTime("2015-04-07 21:18:00.229876");
        secondNum = Integer.parseInt(secondString);
        testCoordinates.add(new Coordinate("28.615400", "115.959500", secondNum));

        secondString = UserStatus.getUserTime("2015-04-07 20:18:00.229876");
        secondNum = Integer.parseInt(secondString);
        testCoordinates.add(new Coordinate("28.615400", "115.959500", secondNum));

        secondString = UserStatus.getUserTime("2015-04-07 19:18:00.229876");
        secondNum = Integer.parseInt(secondString);
        testCoordinates.add(new Coordinate("28.615400", "115.959500", secondNum));

        secondString = UserStatus.getUserTime("2015-04-07 04:18:00.229876");
        secondNum = Integer.parseInt(secondString);
        testCoordinates.add(new Coordinate("28.615400", "115.959500", secondNum));

        secondString = UserStatus.getUserTime("2015-04-07 05:18:00.229876");
        secondNum = Integer.parseInt(secondString);
        testCoordinates.add(new Coordinate("28.615400", "115.959500", secondNum));

        secondString = UserStatus.getUserTime("2015-04-07 06:18:00.229876");
        secondNum = Integer.parseInt(secondString);
        testCoordinates.add(new Coordinate("28.615400", "115.959500", secondNum));

        secondString = UserStatus.getUserTime("2015-04-07 13:18:00.229876");
        secondNum = Integer.parseInt(secondString);
        testCoordinates.add(new Coordinate("28.615400", "115.959500", secondNum));

        secondString = UserStatus.getUserTime("2015-04-07 08:18:00.229876");
        secondNum = Integer.parseInt(secondString);
        testCoordinates.add(new Coordinate("28.615400", "115.959500", secondNum));

        secondString = UserStatus.getUserTime("2015-04-07 09:18:00.229876");
        secondNum = Integer.parseInt(secondString);
        testCoordinates.add(new Coordinate("28.615400", "115.959500", secondNum));

        secondString = UserStatus.getUserTime("2015-04-07 00:18:00.229876");
        secondNum = Integer.parseInt(secondString);
        testCoordinates.add(new Coordinate("28.615400", "115.959500", secondNum));

        for (Coordinate i : testCoordinates) {
            System.out.println("TIME:" + i.time);
        }

        System.out.println();
        Collections.sort(testCoordinates);

        for (Coordinate i : testCoordinates) {
            System.out.println("TIME:" + i.time);
        }


        System.out.println("VALID TIME:" + UserStatus.getWorkTimePoint(testCoordinates));
    }

    @Test
    public void testJudgeUserPlace() throws Exception {
        System.out.println("PLACE:" + UserStatus.judgeUserPlace("2015-04-07 20:00:00.229876"));
    }



} 
