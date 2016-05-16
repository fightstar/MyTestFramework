

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

import DataSourcingPack.PnL_Credit.FilterConfigClasses.FilterType;
import DataSourcingPack.PnL_Credit.FilterConfigClasses.FilterValueType;
import DataSourcingPack.PnL_Credit.FilterConfigClasses.FiltersType;
import DataSourcingPack.PnL_Credit.JsonObjectClasses.FormJsonSource;
import DataSourcingPack.PnL_Credit.JsonObjectClasses.PnLReport;
import DataSourcingPack.PnL_Credit.JsonObjectClasses.Trades;
import UtilsPack.Common;
import UtilsPack.Report;
import UtilsPack.TemplateActions;
import UtilsPack.TimeCounter;
import UtilsPack.UtilsRemoteHost;
import UtilsPack.UtilsSQL;
import UtilsPack.WorkWithProperties;

/**
 * Class that realizes classic assertion-style test approach for validate Filtering.
 * 
 * @BeforeSuit annotated method is used to initialize variables that are used
 *             during test suite and test data preparation: - get SQL
 *               connection - put json on datagw - put dataset on hadoop - run
 *               Insert script - waiting for processing dataset - get workflow
 *               id
 * @BeforeMethod annotated method is used to initialize variables that are used
 *               during every single test and run GetValuation.sh to get xml for test - copy folder
 *               with needed xml to local and parse it. Json parsing.
 * @Test annotated methods uses pre-prepeared data(objects) to do assertions and
 *       create reports.
 */
public class NYFilteringTest extends TestHelper {
	
	//variables
	public static final String COMMONTESTFOLDER = "/03_Filtering/";
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
				+ DateFormat.format(reportDate) + "_filtering"+ ".xlsx");
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
					// comment this section to debug without insertion and processing files
					//==============================
					result += uploadToUnix(fileTestName, jsonFilePathToCopy);
					uploadToHadoop();
					//==============================
					props.put("FEEDID", "18"); // 7 LONDON 18 NY
					props.put("ORGLSRCID", "CREDIT NY");
					props.put("FEEDENTITYID", "DBNY_CAP_LCH");//TraderBookCode from json
					//=========================================
					//executeInsert
					result += executeInsert();
					//wait 8 min 
					try {
						Thread.sleep(480000);
					} catch (InterruptedException e) {
						Common.print("InterruptedException in waiting for processing");
					}
					
					//==========================================
				}
				
				try	{
					Assert.assertFalse(result.contains("Result:false"));
					TemplateActions.updateReport(reportFile, "Bulk upload of files", "PASS", "is sucessful " + result);
				}
				catch (AssertionError e)
				{	
					TemplateActions.updateReport(reportFile, "Bulk upload of files", "FAIL", "is failed" +  result);
				}
				//================================================
				// wait for processing the last uploaded dataset
				TemplateActions.updateReport(reportFile, "Wait for processing", waitingForProcessingDataSet());
				// get collection of workflow id and datasets 
				mapOfWorkflowId = getMapOfWorflowIdAndDataSets(listOfJsonDatasets);
				//================================================
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
		//=============================================================================
		//run GetValuations for needed workflowId and get files to local
		for(String dataset : mapOfWorkflowId.keySet()){
			if(dataset.contains(fileName)){
				runGetValuations(mapOfWorkflowId.get(dataset));
				addToReport("WorkflowID", "=====>", mapOfWorkflowId.get(dataset), reportFile);
				Common.print("GetValuations was run for " + "workflowid " + mapOfWorkflowId.get(dataset) + " and file " + dataset);
				props.put("TADSFOLDERONUNIX", mapOfWorkflowId.get(dataset).substring(0, 7));
			} 
		}
		//==============================================================================
		
		//****************************************************
		//runGetValuations("0045360-160118164119819-oozie-oozi-W");
		//props.put("TADSFOLDERONUNIX", "0045360");
		//*****************************************************
		
		//get json objects from testDataFolder and parse it
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
	
	//test
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
	public void legalEntity_1(){
		Map<String, String> results = new LinkedHashMap<String, String>();
		folderWithTadsOnUnix = props.getProperty("TADSFOLDERONUNIX");
		downloadFromUnixToLocal(folderWithTadsOnUnix);
		listOfXmlObjects = getTaMessages(localPath, results);
		mapOfXmlObjects = getCompareMapOfXmlObjects();
		ensureThatFileWasNotFiltered(results ,1);
//		List<List<String>> outOfSkopeList = null;
//		outOfSkopeList = getXLSdata(testDataFolder+"LegalEntity.xlsx");
		verifyFilteringLegalEntity(jsonForTest, mapOfXmlObjects,results, "in");
		updateReportWithTestResults(results);
		updateReportToFinal(reportFile, results, startTime);
	}

	
	@Test
	public void legalEntity_2(){
		Map<String, String> results = new LinkedHashMap<String, String>();
		folderWithTadsOnUnix = props.getProperty("TADSFOLDERONUNIX");
		downloadFromUnixToLocal(folderWithTadsOnUnix);
		listOfXmlObjects = getTaMessages(localPath, results);
		mapOfXmlObjects = getCompareMapOfXmlObjects();
		ensureThatFileWasNotFiltered(results, 1);
		List<List<String>> outOfSkopeList = null;
		verifyFilteringLegalEntity(jsonForTest, mapOfXmlObjects,results, "in");
		updateReportWithTestResults(results);
		updateReportToFinal(reportFile, results, startTime);
	}
	
	@Test
	public void legalEntity_3(){
		Map<String, String> results = new LinkedHashMap<String, String>();
		folderWithTadsOnUnix = props.getProperty("TADSFOLDERONUNIX");
		downloadFromUnixToLocal(folderWithTadsOnUnix);
		listOfXmlObjects = getTaMessages(localPath, results);
		mapOfXmlObjects = getCompareMapOfXmlObjects();
		ensureThatFileWasNotFiltered(results, 1);
		List<List<String>> outOfSkopeList = null;
		verifyFilteringLegalEntity(jsonForTest, mapOfXmlObjects,results, "in");
		updateReportWithTestResults(results);
		updateReportToFinal(reportFile, results, startTime);
	}
    @Test
	public void legalEntity_7() {
		Common.print("legalEntity_7 test started");
		Map<String, String> results = new LinkedHashMap<String, String>();
		folderWithTadsOnUnix = props.getProperty("TADSFOLDERONUNIX");
		downloadFromUnixToLocalForFiltering(folderWithTadsOnUnix);
		listOfXmlObjects = getTaMessages(localPath, results);
		mapOfXmlObjects = getCompareMapOfXmlObjects();
		verifyFilteringLegalEntity(jsonForTest, mapOfXmlObjects,results, "not in");
		updateReportWithTestResults(results);
		updateReportToFinal(reportFile, results, startTime);
		Common.print("legalEntity_7 test finished");
	}
    
    @Test
    public void checkConfigurationFiles(){
    	Common.print("checkConfigurationFiles test started");
		Map<String, String> results = new LinkedHashMap<String, String>();
    	//get xml file from hadoop
    	String s = getFilterConfigurationXMLFromHadoop();
    	Common.print(s);
    	//copy file from unix to local
    	String localPath = basicPath + "\\temp";
		String result = "";
		UtilsRemoteHost rh = new UtilsRemoteHost(props.get("HDFS_USER")
				.toString(), props.get("HDFS_PASS").toString(), props.get(
				"HDFS_URL").toString());
	
		result  = rh.FileCopyFrom(localPath + "\\", props.getProperty("UNIX_PATH_DATA")
				+ "/" + "credit_ods_filters_ny.xml");
		rh.closeSession();
    	//parse xml 
		FiltersType filters = xmlParsinginFiltersObjects(basicPath + "\\temp"+"\\credit_ods_filters_ny.xml");
    	//verify
    	verifyThatConfigurationFileContainsLegalEntities(filters, results);
    	updateReportWithTestResults(results);
		updateReportToFinal(reportFile, results, startTime);
		Common.print("checkConfigurationFiles test finished");
    }
    private Map<String, String>  verifyThatConfigurationFileContainsLegalEntities(FiltersType filters, Map<String, String> results) {
    	String[] outOfScopeList = {"0861","0839","0550","6201","6401","5006"};
    	FilterType filter = filters.getFilter().get(0);
    	if(filter.getFilterDescription().equals("LegalEntityFilter")){
    		for(FilterValueType type : filter.getFilterValue()){
    			for(String entity : outOfScopeList){
    				if (type.getValue().equals(entity)){
    					if(type.isInScope()){
    						results.put(type.getValue() +" in scope", "PASS");
    					}else if (!type.isInScope()) {
    						results.put("not in scope", "FAIL");
    						}
    					Common.print("Entity " + entity + "filter value "+ type.getValue());
    					}
    				}
    			}
    		} else Common.print("<filter_description> is" + filter.getFilterDescription());
		return results;
	}

	@Test
    public void settlementSourseSysytemFilteringTest() {
    	System.out.println("settlementSourseSysytemFilteringTest test started");
		Map<String, String> results = new LinkedHashMap<String, String>();
		folderWithTadsOnUnix = props.getProperty("TADSFOLDERONUNIX");
		//get list of out of scope data
		String [] outOfScope  = {"BOND", "BSK_CLN", "CASHCOLLAT", "CD_GTEE", "COM_FDW", "COM_SWP", "DEF_PORT", "EQ_OPT", "EQ_SWP", "FX_OPT", "GENTROR_SW", "IR_ETO ", "RESERVES", "FEES"};
		//step 3
		downloadFromUnixToLocal(folderWithTadsOnUnix);
		listOfXmlObjects = getTaMessages(localPath, results);
		mapOfXmlObjects = getCompareMapOfXmlObjects();
		ensureThatFileWasNotFiltered(results, 3);
		verifyFilteringSourseSystem(jsonForTest, mapOfXmlObjects, outOfScope, results, false); //out of scope
		cleanLocalTempDirectory(localPath);
		//step 4
		downloadFromUnixToLocalForFiltering(folderWithTadsOnUnix);
		listOfXmlObjects = getTaMessages(localPath, results);
		mapOfXmlObjects = getCompareMapOfXmlObjects();
		verifyFilteringSourseSystem(jsonForTest, mapOfXmlObjects, outOfScope, results, true); //in scope
		updateReportWithTestResults(results);
		updateReportToFinal(reportFile, results, startTime);
		System.out.println("settlementSourseSysytemFilteringTest test finished");
    }
    
    @Test
    public void productFilteringTestIN() {
    	System.out.println("productFilteringTest test started");
		Map<String, String> results = new LinkedHashMap<String, String>();
		folderWithTadsOnUnix = props.getProperty("TADSFOLDERONUNIX");
		downloadFromUnixToLocal(folderWithTadsOnUnix);
		listOfXmlObjects = getTaMessages(localPath, results);
		mapOfXmlObjects = getCompareMapOfXmlObjects();
		ensureThatFileWasNotFiltered(results, 251);
		updateReportWithTestResults(results);
		updateReportToFinal(reportFile, results, startTime);
		System.out.println("productFilteringTest test finished");
    }
    @Test
    public void productFilteringTestOUT() {
    	System.out.println("productFilteringTest test started");
		Map<String, String> results = new LinkedHashMap<String, String>();
		folderWithTadsOnUnix = props.getProperty("TADSFOLDERONUNIX");
		downloadFromUnixToLocalForFiltering(folderWithTadsOnUnix);
		listOfXmlObjects = getTaMessages(localPath, results);
		mapOfXmlObjects = getCompareMapOfXmlObjects();
		verifyFilteringProducts(jsonForTest, mapOfXmlObjects, results);
		updateReportWithTestResults(results);
		updateReportToFinal(reportFile, results, startTime);
		System.out.println("productFilteringTest test finished");
    }
    @Test
    public void reportableAmountTypeFilteringTest(){
    	Common.print("productFilteringTest test started");
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
		Common.print("productFilteringTest test finished");
    }
    @Test
    public void filteringOnValuationEngineTest_1() {
    	Common.print("filteringOnValuationEngineTest_1 test started");
		Map<String, String> results = new LinkedHashMap<String, String>();
		folderWithTadsOnUnix = props.getProperty("TADSFOLDERONUNIX");
		downloadFromUnixToLocal(folderWithTadsOnUnix);
		listOfXmlObjects = getTaMessages(localPath, results);
		mapOfXmlObjects = getCompareMapOfXmlObjects();
		ensureThatFileWasNotFiltered(results, 1);
		updateReportWithTestResults(results);
		updateReportToFinal(reportFile, results, startTime);
		Common.print("filteringOnValuationEngineTest_1 test finished");
    }
    @Test
    public void filteringOnValuationEngineTest_2() {
    	Common.print("filteringOnValuationEngineTest_2 test started");
		Map<String, String> results = new LinkedHashMap<String, String>();
		folderWithTadsOnUnix = props.getProperty("TADSFOLDERONUNIX");
		downloadFromUnixToLocal(folderWithTadsOnUnix);
		listOfXmlObjects = getTaMessages(localPath, results);
		mapOfXmlObjects = getCompareMapOfXmlObjects();
		ensureThatFileWasNotFiltered(results, 1);
		updateReportWithTestResults(results);
		updateReportToFinal(reportFile, results, startTime);
		Common.print("filteringOnValuationEngineTest_2 test finished");
    }
    @Test
    public void filteringOnValuationEngineTest_3() {
    	Common.print("filteringOnValuationEngineTest_3 test started");
		Map<String, String> results = new LinkedHashMap<String, String>();
		folderWithTadsOnUnix = props.getProperty("TADSFOLDERONUNIX");
		downloadFromUnixToLocal(folderWithTadsOnUnix);
		listOfXmlObjects = getTaMessages(localPath, results);
		mapOfXmlObjects = getCompareMapOfXmlObjects();
		ensureThatFileWasNotFiltered(results, 1);
		updateReportWithTestResults(results);
		updateReportToFinal(reportFile, results, startTime);
		Common.print("filteringOnValuationEngineTest_3 test finished");
    }
    @Test
    public void filteringOnValuationEngineTest_4() {
    	Common.print("filteringOnValuationEngineTest_4 test started");
		Map<String, String> results = new LinkedHashMap<String, String>();
		folderWithTadsOnUnix = props.getProperty("TADSFOLDERONUNIX");
		downloadFromUnixToLocal(folderWithTadsOnUnix);
		listOfXmlObjects = getTaMessages(localPath, results);
		mapOfXmlObjects = getCompareMapOfXmlObjects();
		ensureThatFileWasNotFiltered(results, 1);
		updateReportWithTestResults(results);
		updateReportToFinal(reportFile, results, startTime);
		Common.print("filteringOnValuationEngineTest_4 test finished");
    }
}

