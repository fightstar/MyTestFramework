

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import DataSourcingPack.PnL_Credit.JsonObjectClasses.FormJsonSource;
import DataSourcingPack.PnL_Credit.JsonObjectClasses.PnLReport;
import UtilsPack.Common;
import UtilsPack.Report;
import UtilsPack.TemplateActions;
import UtilsPack.TimeCounter;
import UtilsPack.UtilsSQL;
import UtilsPack.WorkWithProperties;

public class LDNFilteringTest extends TestHelper {
	//variables
		public static final String COMMONTESTFOLDER = "/Dataset Filtering London/";
		public static String localPath;
		private static ArrayList<String> listOfJsonDatasets = new ArrayList<String>();
		ArrayList<PnLReport> listWithJsonsObjects;
		Map<String, PnLReport> mapOfJsons = new HashMap<String, PnLReport>();
		private static Map <String, String> mapOfWorkflowId;
		private long totalSuiteDurationStart;
		
		public String folderWithTadsOnUnix;
		String testDataFolder;
		
		//before
		@BeforeSuite
		@Parameters({ "configFile" })
		public void testPreparation(String configFile) throws FileNotFoundException {
			totalSuiteDurationStart = TimeCounter.getCurrentTime();
			System.out.println("===================> before suit started");

			props = WorkWithProperties.LoadProps(".\\TestData\\" + configFile); // read
																				// properties
																				// file
			basicPath = props.getProperty("PATH");
			localPath = basicPath + "\\temp";
			unixFolderPath = props.getProperty("UNIX_PATH_DATA");
			props.put(
					"SCRIPTS_FOLDER",
					"C:\\Users\\shevvla\\SVN\\Automation_Luxoft\\TA\\TestData\\DS\\PnL_Credit\\Scripts");
			ScriptsPath = props.getProperty("SCRIPTS_FOLDER");
			// report
			BUSINESS_LINE = props.getProperty("BUSINESS_LINE");
			Date reportDate = new Date();
			reportFile = new Report(".\\Reports\\" + BUSINESS_LINE + "_"
					+ DateFormat.format(reportDate) + "_LDN_filtering"+ ".xlsx");
			startTime = TimeCounter.getCurrentTime();
			String result = "";
			date = new Date();
			// get filenames
					List<String> jsonSourcesFilesWithPath = getJsonFileInDir(basicPath+COMMONTESTFOLDER);
					Common.print(jsonSourcesFilesWithPath.size());	
					reportFile.addHeader("Files upload and processing");
					for (String jsonSource : jsonSourcesFilesWithPath){
						UtilsSQL.getDBConnection(props);
						fileTestName = Common.updateFileName(Common.getFileNameFromAbsPath(jsonSource),"_" + DateFormat.format(date));
						props.put("JSON_DATASET", fileTestName);
						listOfJsonDatasets.add(fileTestName);
						jsonFilePathToCopy = jsonSourcesFilesWithPath.get(jsonSourcesFilesWithPath.indexOf(jsonSource));
						TemplateActions.updateReport(reportFile, "Json files", "PASS",
								"json file " + jsonFilePathToCopy
										+ " was uploaded");
						result += uploadToUnix(fileTestName, jsonFilePathToCopy);
						uploadToHadoop();
						props.put("FEEDID", "7"); // 7 LONDON 18 NY
						props.put("ORGLSRCID", "CREDIT");
						props.put("FEEDENTITYID", "DBNY_CAP_LCH");//TraderBookCode from json
						//executeInsert
						result += executeInsert();
						//wait 6 min 
						try {
							Thread.sleep(400000);
						} catch (InterruptedException e) {
							Common.print("InterruptedException in waiting for processing");
						}
						
					}
					
					try	{
						Assert.assertFalse(result.contains("Result:false"));
						TemplateActions.updateReport(reportFile, "Bulk upload of files", "PASS", "is sucessful " + result);
					}
					catch (AssertionError e)
					{	
						TemplateActions.updateReport(reportFile, "Bulk upload of files", "FAIL", "is failed" +  result);
					}
					// wait for processing the last uploaded dataset
					TemplateActions.updateReport(reportFile, "Wait for processing", waitingForProcessingDataSet());
					// get collection of workflow id and datasets 
					mapOfWorkflowId = getMapOfWorflowIdAndDataSets(listOfJsonDatasets);
					reportFile.addFinal("PASS",startTime);
			
			
			System.out.println("===================> before suit finished");
		}
		
		@BeforeMethod
		@Parameters(value = {"fileName", "testFolderName"})
		public void testDataPreparationStep(String fileName, String testFolderName) {
			cleanLocalTempDirectory(localPath);
			Common.print("===================> before method started");
			testDataFolder = basicPath+COMMONTESTFOLDER+testFolderName+"\\";
			reportFile.addHeader(testFolderName+" "+fileName);
			Common.print(fileName);
			startTime = TimeCounter.getCurrentTime();
			
			//run GetValuations for needed workflowId and get files to local
			for(String dataset : mapOfWorkflowId.keySet()){
				if(dataset.contains(fileName)){
					runGetValuations(mapOfWorkflowId.get(dataset));
					addToReport("WorkflowID", "=====>", mapOfWorkflowId.get(dataset), reportFile);
					Common.print("GetValuations was run for " + "workflowid " + mapOfWorkflowId.get(dataset) + " and file " + dataset);
					props.put("TADSFOLDERONUNIX", mapOfWorkflowId.get(dataset).substring(0, 7));
				} 
			}
			//get json objects from testDataFolder and parse it
			
			//FOR DEBUG PURPOSES
			//props.put("TADSFOLDERONUNIX", "");
			try {
				FormJsonSource formJsonSource = new FormJsonSource(testDataFolder, fileName);
				jsonForTest = formJsonSource.formReportObjectFromJson();
				Common.print("Trades in jsonForTest: " + jsonForTest.toString());
			} catch (Exception e) {
				e.printStackTrace();
				TemplateActions.updateReport(reportFile, "Json parsing", "FAIL",
						"json isn't parsed");
			}
			Common.print("===================> before method finished");
		}
		
		//after
		@AfterMethod
		public void afterTest() {
			Common.print("===================>After method");
			// close sql connection
			UtilsSQL.closeConnection();
			// delete folder on Unix after test
			deleteDirOnUnix();
			rh.closeSession();
			cleanLocalTempDirectory(localPath);

		}
		@AfterSuite
		public void afterSuite() {
			reportFile.addHeader("SUITE DURATION");
			reportFile.addFinal("=",totalSuiteDurationStart);
		}
		
		@Test
		public void amountFilteringTest() {
			Common.print("amountFilteringTest test started");
			Map<String, String> results = new LinkedHashMap<String, String>();
			folderWithTadsOnUnix = props.getProperty("TADSFOLDERONUNIX");
			
			//step 3
			downloadFromUnixToLocal(folderWithTadsOnUnix);
			listOfXmlObjects = getTaMessages(localPath, results);
			mapOfXmlObjects = getCompareMapOfXmlObjects();
			ensureThatFileWasNotFiltered(results, 1);
			
			cleanLocalTempDirectory(localPath);
			//step 4
			
			downloadFromUnixToLocalForFiltering(folderWithTadsOnUnix);
			listOfXmlObjects = getTaMessages(localPath, results);
			mapOfXmlObjects = getCompareMapOfXmlObjects();
			//verify
			
			verifyFilteringAmount(jsonForTest, mapOfXmlObjects, results);
			
			
			
			updateReportWithTestResults(results);
			updateReportToFinal(reportFile, results, startTime);
			Common.print("amountFilteringTest test finished");
		}
		
		@Test
	    public void productFilteringFinancialProductTypeTestIN() {
			Map<String, String> results = new LinkedHashMap<String, String>();
			folderWithTadsOnUnix = props.getProperty("TADSFOLDERONUNIX");
			downloadFromUnixToLocal(folderWithTadsOnUnix);
			listOfXmlObjects = getTaMessages(localPath, results); //TODO debug and handle null value
			mapOfXmlObjects = getCompareMapOfXmlObjects();
			ensureThatFileWasNotFiltered(results, 162);
			updateReportWithTestResults(results);
			updateReportToFinal(reportFile, results, startTime);
	    }
		
	    @Test
	    public void productFilteringFinancialProductTypeTestOUT() {
			Map<String, String> results = new LinkedHashMap<String, String>();
			folderWithTadsOnUnix = props.getProperty("TADSFOLDERONUNIX");
			downloadFromUnixToLocalForFiltering(folderWithTadsOnUnix);
			listOfXmlObjects = getTaMessages(localPath, results);
			mapOfXmlObjects = getCompareMapOfXmlObjects();
			verifyFilteringProducts(jsonForTest, mapOfXmlObjects, results);
			updateReportWithTestResults(results);
			updateReportToFinal(reportFile, results, startTime);
	    }
	    
	    @Test
	    public void legalEntityFilteringTestIN() {
	    	//Filtering_LegalEntity_in - 0840
	    	Map<String, String> results = new LinkedHashMap<String, String>();
			folderWithTadsOnUnix = props.getProperty("TADSFOLDERONUNIX");
			downloadFromUnixToLocal(folderWithTadsOnUnix);
			listOfXmlObjects = getTaMessages(localPath, results);
			mapOfXmlObjects = getCompareMapOfXmlObjects();	
			ensureThatFileWasNotFiltered(results, 1);
			updateReportWithTestResults(results);
			updateReportToFinal(reportFile, results, startTime);
	    }
	    
	    @Test
	    public void legalEntityFilteringTestOUT() {
	    	//Filtering_LegalEntity_out - not 0840
	    	Map<String, String> results = new LinkedHashMap<String, String>();
			folderWithTadsOnUnix = props.getProperty("TADSFOLDERONUNIX");
			downloadFromUnixToLocalForFiltering(folderWithTadsOnUnix);
			listOfXmlObjects = getTaMessages(localPath, results);
			ensureThatFileWasFiltered(results, 1);
			updateReportWithTestResults(results);
			updateReportToFinal(reportFile, results, startTime);
	    }
	    
	    @Test
	    public void prdsProductNameFilteringTestIN() {
	    	Map<String, String> results = new LinkedHashMap<String, String>();
			folderWithTadsOnUnix = props.getProperty("TADSFOLDERONUNIX");
			downloadFromUnixToLocal(folderWithTadsOnUnix);
			listOfXmlObjects = getTaMessages(localPath, results);
			System.out.println("LIST OF XML OBJECT SIZE = " + listOfXmlObjects.size());
			mapOfXmlObjects = getCompareMapOfXmlObjects();
			ensureThatFileWasNotFiltered(results, 162);
			updateReportWithTestResults(results);
			updateReportToFinal(reportFile, results, startTime);
	    }
	    
	    @Test
	    public void prdsProductNameFilteringTestOUT() {
	    	Map<String, String> results = new LinkedHashMap<String, String>();
			folderWithTadsOnUnix = props.getProperty("TADSFOLDERONUNIX");
			downloadFromUnixToLocalForFiltering(folderWithTadsOnUnix);
			listOfXmlObjects = getTaMessages(localPath, results);
			mapOfXmlObjects = getCompareMapOfXmlObjects();
			verifyFilteringProducts(jsonForTest, mapOfXmlObjects, results);
			updateReportWithTestResults(results);
			updateReportToFinal(reportFile, results, startTime);
	    }
	    
	    @Test
	    public void ratFilteringTest() {
			Map<String, String> results = new LinkedHashMap<String, String>();
			folderWithTadsOnUnix = props.getProperty("TADSFOLDERONUNIX");
			downloadFromUnixToLocal(folderWithTadsOnUnix);
			listOfXmlObjects = getTaMessages(localPath, results);
			mapOfXmlObjects = getCompareMapOfXmlObjects();
			verifyFilteringReportableAmount(jsonForTest, mapOfXmlObjects, results, true);
			cleanLocalTempDirectory(localPath);
			downloadFromUnixToLocalForFiltering(folderWithTadsOnUnix);
			listOfXmlObjects = getTaMessages(localPath, results);
			mapOfXmlObjects = getCompareMapOfXmlObjects();
			verifyFilteringReportableAmount(jsonForTest, mapOfXmlObjects, results, false);
			updateReportWithTestResults(results);
			updateReportToFinal(reportFile, results, startTime);
	    }
	    
	    @Test
	    public void settlementSSysFilteringTest() {
	    	Map<String, String> results = new LinkedHashMap<String, String>();
			folderWithTadsOnUnix = props.getProperty("TADSFOLDERONUNIX");
			downloadFromUnixToLocal(folderWithTadsOnUnix);
			listOfXmlObjects = getTaMessages(localPath, results);
			mapOfXmlObjects = getCompareMapOfXmlObjects();
			verifyFilteringSourseSystem(jsonForTest, mapOfXmlObjects, results);
			cleanLocalTempDirectory(localPath);
			downloadFromUnixToLocalForFiltering(folderWithTadsOnUnix);
			listOfXmlObjects = getTaMessages(localPath, results);
			mapOfXmlObjects = getCompareMapOfXmlObjects();
			verifyFilteringSourseSystem(jsonForTest, mapOfXmlObjects, results);
			updateReportWithTestResults(results);
			updateReportToFinal(reportFile, results, startTime);
	    }
	    
	    @Test
	    public void filteringOnValuationEngineTest_1() {
			Map<String, String> results = new LinkedHashMap<String, String>();
			folderWithTadsOnUnix = props.getProperty("TADSFOLDERONUNIX");
			downloadFromUnixToLocal(folderWithTadsOnUnix);
			listOfXmlObjects = getTaMessages(localPath, results);
			mapOfXmlObjects = getCompareMapOfXmlObjects();
			ensureThatFileWasNotFiltered(results, 1);
			updateReportWithTestResults(results);
			updateReportToFinal(reportFile, results, startTime);
	    }
	    
	    @Test
	    public void filteringOnValuationEngineTest_2() {
			Map<String, String> results = new LinkedHashMap<String, String>();
			folderWithTadsOnUnix = props.getProperty("TADSFOLDERONUNIX");
			downloadFromUnixToLocal(folderWithTadsOnUnix);
			listOfXmlObjects = getTaMessages(localPath, results);
			mapOfXmlObjects = getCompareMapOfXmlObjects();
			ensureThatFileWasNotFiltered(results, 1);
			updateReportWithTestResults(results);
			updateReportToFinal(reportFile, results, startTime);
	    }
	    
	    @Test
	    public void filteringOnValuationEngineTest_3() {
			Map<String, String> results = new LinkedHashMap<String, String>();
			folderWithTadsOnUnix = props.getProperty("TADSFOLDERONUNIX");
			downloadFromUnixToLocal(folderWithTadsOnUnix);
			listOfXmlObjects = getTaMessages(localPath, results);
			mapOfXmlObjects = getCompareMapOfXmlObjects();
			ensureThatFileWasNotFiltered(results, 1);
			updateReportWithTestResults(results);
			updateReportToFinal(reportFile, results, startTime);
	    }
	    
	    @Test
	    public void filteringOnValuationEngineTest_4() {
			Map<String, String> results = new LinkedHashMap<String, String>();
			folderWithTadsOnUnix = props.getProperty("TADSFOLDERONUNIX");
			downloadFromUnixToLocalForFiltering(folderWithTadsOnUnix);
			listOfXmlObjects = getTaMessages(localPath, results);
			mapOfXmlObjects = getCompareMapOfXmlObjects();
			ensureThatFileWasFiltered(results, 1);
			updateReportWithTestResults(results);
			updateReportToFinal(reportFile, results, startTime);
	    }
	    
	    @Test
	    public void filteringOnValuationEngineTest_5() {
			Map<String, String> results = new LinkedHashMap<String, String>();
			folderWithTadsOnUnix = props.getProperty("TADSFOLDERONUNIX");
			downloadFromUnixToLocalForFiltering(folderWithTadsOnUnix);
			listOfXmlObjects = getTaMessages(localPath, results);
			mapOfXmlObjects = getCompareMapOfXmlObjects();
			ensureThatFileWasFiltered(results, 1);
			updateReportWithTestResults(results);
			updateReportToFinal(reportFile, results, startTime);
	    }
	    
	    @Test
	    public void filteringOnValuationEngineTest_6() {
			Map<String, String> results = new LinkedHashMap<String, String>();
			folderWithTadsOnUnix = props.getProperty("TADSFOLDERONUNIX");
			downloadFromUnixToLocalForFiltering(folderWithTadsOnUnix);
			listOfXmlObjects = getTaMessages(localPath, results);
			mapOfXmlObjects = getCompareMapOfXmlObjects();
			ensureThatFileWasFiltered(results, 1);
			updateReportWithTestResults(results);
			updateReportToFinal(reportFile, results, startTime);
	    }
}
