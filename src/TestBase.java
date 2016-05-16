
import UtilsPack.Report;
import org.testng.Assert;
import org.testng.ITestResult;
import org.testng.Reporter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by vshevchenko.
 */
public class TestBase{

    private static Map<ITestResult, List<Throwable>> verificationFailuresMap = new HashMap<ITestResult, List<Throwable>>();
    private static String verificationFailure;




    public static String assertEquals(String actual, String expected, String result) {

        try{
            Assert.assertEquals(actual, expected);
            result = "TRUE";
        } catch(Throwable e) {
            addVerificationFailure(e);
            result = "FALSE";

        }
        return result;
    }
    public static String assertEquals(String actual, String expected) {
        String result = "";
        try{
            Assert.assertEquals(actual, expected);
            result = "PASS";
        } catch(Throwable e) {
            addVerificationFailureString(e);
            result = "FAIL";
        }
        return result;
    }

    public static List getVerificationFailures() {
        List verificationFailures = verificationFailuresMap.get(Reporter.getCurrentTestResult());
        return verificationFailures == null ? new ArrayList() : verificationFailures;
    }

    private static void addVerificationFailure(Throwable e) {
        List verificationFailures = getVerificationFailures();
        verificationFailuresMap.put(Reporter.getCurrentTestResult(), verificationFailures);
        verificationFailures.add(e);
    }

    public static String getVerificationFailure(){
        return verificationFailure;
    }

    private static void addVerificationFailureString(Throwable e){
        verificationFailure = e.toString();

    }
}
