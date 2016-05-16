

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

public class NYEnrichmentTestNEW extends TestHelper {
	//variables
	public static final String COMMONTESTFOLDER = "/02_Enrichment/";
	public static String localPath;
	private static ArrayList<String> listOfJsonDatasets = new ArrayList<String>();
	ArrayList<PnLReport> listWithJsonsObjects;
	Map<String, PnLReport> mapOfJsons = new HashMap<String, PnLReport>();
	private static Map <String, String> mapOfWorkflowId;
	private long totalSuiteDurationStart;
	
	public String folderWithTadsOnUnix;
	String testDataFolder;
	private Map<String, String> productFinancialMapping = new HashMap<String, String>();
	private Map<String, String> productPRDSmapping = new HashMap<String, String>();
	String countOfEnrichedTads;
	
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
				+ DateFormat.format(reportDate)+ "_enrichment" + ".xlsx");
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
					executeInsert();
					//wait 8 min 
					
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
				if(waitingForProcessingDataSet().contains("FAIL")){
					reportFile.addFinal("FAIL",startTime);
				}
				else {
				
				// get collection of workflow id and datasets 
				mapOfWorkflowId = getMapOfWorflowIdAndDataSets(listOfJsonDatasets);
				//================================================
				reportFile.addFinal("PASS",startTime);
				}
		
		
		System.out.println("===================> before suit finished");
	}
	
	@BeforeMethod
	@Parameters(value = {"fileName", "testFolderName"})
	public void testDataPreparationStep(String fileName,String testFolderName) {
		Common.print("===================> before method started");
		cleanLocalTempDirectory(localPath);
		testDataFolder = basicPath+COMMONTESTFOLDER+testFolderName+"\\";
		reportFile.addHeader(testFolderName+" "+fileName);
		Common.print(fileName);
		startTime = TimeCounter.getCurrentTime();
		//=============================================================================
		//run GetValuations for needed workflowId and get files to local
		
		
		
		//select HDFS_FLDR_NME from fim_srvc_instc where workflow_id = %WORKFLOWID% and FIM_SRVC_NME= 'SRV_FILPUB'; 
		

		//hadoop fs -get <value from HDFS_FLDR_NAME>
		
		//data folder inside downloaded data and check files with prefix feed_instc_id like 2612_1459163740816-r-00000

		
		
		for(String dataset : mapOfWorkflowId.keySet()){
			if(dataset.contains(fileName)){
				props.put("WORKFLOW_ID", mapOfWorkflowId.get(dataset));
				props.put("HDFS_FLDR_NAME", getHDFSfolderName());
				//create folder on Unix and run /hadoop fs -get <value from HDFS_FLDR_NAME>
				createFolderAndgetTADSfromHDFS();
				
				//runGetValuations(mapOfWorkflowId.get(dataset));
				addToReport("WorkflowID", "=====>", mapOfWorkflowId.get(dataset), reportFile);
				//Common.print("GetValuations was run for " + "workflowid " + mapOfWorkflowId.get(dataset) + " and file " + dataset);
				props.put("TADSFOLDERONUNIX", mapOfWorkflowId.get(dataset).substring(0, 7));
			} 
		}
		//==============================================================================
		
		//****************************************************
		/*runGetValuations("0054888-160118164119819-oozie-oozi-W");
		props.put("TADSFOLDERONUNIX", "0054888");*/
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
	
	@Test
	public void productEnrichmentTest_PRDS(){
		Common.print("productEnrichmentTest_PRDS test started");
		Map<String, String> results = new LinkedHashMap<String, String>();
		folderWithTadsOnUnix = props.getProperty("TADSFOLDERONUNIX");
		productPRDSmapping = getMapXLSdata(testDataFolder+"PRDSProductTypeMapping.xlsx");
		
		downloadFromUnixToLocal(folderWithTadsOnUnix);
		
		listOfXmlObjects = getTaMessages(localPath, results);
		mapOfXmlObjects = getCompareMapOfXmlObjects();
		countOfEnrichedTads = verifyEnrichmentPRDS(jsonForTest, mapOfXmlObjects, productPRDSmapping, results);
		
		results.put("All files was enriched", verifyThatAllFilesWasEnriched(mapOfXmlObjects, countOfEnrichedTads));
		updateReportWithTestResults(results);
		updateReportToFinal(reportFile, results, startTime);
		Common.print("productEnrichmentTest_PRDS test finished");
	}
	@Test
	public void productEnrichmentTest_Financial(){
		Common.print("productEnrichmentTest_Financial test started");
		Map<String, String> results = new LinkedHashMap<String, String>();
		folderWithTadsOnUnix = props.getProperty("TADSFOLDERONUNIX");
		productFinancialMapping = getMapXLSdata(testDataFolder+"FinancialProductTypeMapping.xlsx");
		
		downloadFromUnixToLocal(folderWithTadsOnUnix);
		
		listOfXmlObjects = getTaMessages(localPath, results);
		mapOfXmlObjects = getCompareMapOfXmlObjects();
		countOfEnrichedTads = verifyEnrichmentFinancial(jsonForTest, mapOfXmlObjects, productFinancialMapping, results);
		results.put("All files was enriched", verifyThatAllFilesWasEnriched(mapOfXmlObjects, countOfEnrichedTads));
		updateReportWithTestResults(results);
		updateReportToFinal(reportFile, results, startTime);
		Common.print("productEnrichmentTest_Financial test finished");
	}
	@Test
	public void productEnrichmentTest_3(){
		Common.print("productEnrichmentTest_3 test started");
		Map<String, String> results = new LinkedHashMap<String, String>();
		folderWithTadsOnUnix = props.getProperty("TADSFOLDERONUNIX");
		productPRDSmapping = getMapXLSdata(testDataFolder+"PRDSProductTypeMapping.xlsx");
		productFinancialMapping = getMapXLSdata(testDataFolder+"FinancialProductTypeMapping.xlsx");
		
		downloadFromUnixToLocal(folderWithTadsOnUnix);
		
		listOfXmlObjects = getTaMessages(localPath, results);
		mapOfXmlObjects = getCompareMapOfXmlObjects();
		
		verifyEnrichmentCombinated(jsonForTest, mapOfXmlObjects, productPRDSmapping, productFinancialMapping, results);
		updateReportWithTestResults(results);
		updateReportToFinal(reportFile, results, startTime);
		Common.print("productEnrichmentTest_3 test finished");
	}
	@Test
	public void bookEnrichmentTest() {
		Common.print("bookEnrichmentTest test started");
		Map<String, String> results = new LinkedHashMap<String, String>();
		folderWithTadsOnUnix = props.getProperty("TADSFOLDERONUNIX");
		
		downloadFromUnixToLocal(folderWithTadsOnUnix);
		
		listOfXmlObjects = getTaMessages(localPath, results);
		mapOfXmlObjects = getCompareMapOfXmlObjects();
		
		
		verifyBookEnrichment(jsonForTest, listOfXmlObjects, results);
		
		updateReportWithTestResults(results);
		updateReportToFinal(reportFile, results, startTime);
		Common.print("bookEnrichmentTest test finished");
	}
}
