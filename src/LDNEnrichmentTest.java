

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

import UtilsPack.Common;
import UtilsPack.Report;
import UtilsPack.TemplateActions;
import UtilsPack.TimeCounter;
import UtilsPack.UtilsSQL;
import UtilsPack.WorkWithProperties;
import DataSourcingPack.PnL_Credit.JsonObjectClasses.FormJsonSource;
import DataSourcingPack.PnL_Credit.JsonObjectClasses.PnLReport;

public class LDNEnrichmentTest extends TestHelper {
	public static final String COMMONTESTFOLDER = "/Enrichment London/";
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
  					result += uploadToUnix(fileTestName, jsonFilePathToCopy);
					uploadToHadoop();
					props.put("FEEDID", "7"); // 7 LONDON 18 NY
					props.put("ORGLSRCID", "CREDIT NY");
					props.put("FEEDENTITYID", "DBNY_CAP_LCH");//TraderBookCode from json
					//executeInsert
					executeInsert();
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
				if(waitingForProcessingDataSet().contains("FAIL")){
					reportFile.addFinal("FAIL",startTime);
				}
				else {
				
				// get collection of workflow id and datasets 
				mapOfWorkflowId = getMapOfWorflowIdAndDataSets(listOfJsonDatasets);
				
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
	public void bookEnrichmentTestBRDS() {
		Map<String, String> results = new LinkedHashMap<String, String>();
		folderWithTadsOnUnix = props.getProperty("TADSFOLDERONUNIX");
		downloadFromUnixToLocal(folderWithTadsOnUnix);
		listOfXmlObjects = getTaMessages(localPath, results);
		mapOfXmlObjects = getCompareMapOfXmlObjects();
		verifyBookEnrichment(jsonForTest, listOfXmlObjects, results);
		updateReportWithTestResults(results);
		updateReportToFinal(reportFile, results, startTime);
	}
	
	@Test
	public void bookEnrichmentTestNotBRDS() {
		Map<String, String> results = new LinkedHashMap<String, String>();
		folderWithTadsOnUnix = props.getProperty("TADSFOLDERONUNIX");
		downloadFromUnixToLocal(folderWithTadsOnUnix);
		listOfXmlObjects = getTaMessages(localPath, results);
		mapOfXmlObjects = getCompareMapOfXmlObjects();
		if(mapOfXmlObjects.get(jsonForTest.getTraderBook().get(0).getTrades().get(0).getTradeId()).getValuation().getBook()==null){
			results.put("Book entity is not populated in generated TADS-file.", "PASS");
		} else {results.put("Book entity is not populated in generated TADS-file.", "FAIL");}
		updateReportWithTestResults(results);
		updateReportToFinal(reportFile, results, startTime);
	}
	
	@Test
	public void productEnrichmentValidPRDS(){
		Map<String, String> results = new LinkedHashMap<String, String>();
		folderWithTadsOnUnix = props.getProperty("TADSFOLDERONUNIX");
		productPRDSmapping = getMapXLSdata(testDataFolder+"PRDSProductTypeMapping_LDN.xlsx");
		downloadFromUnixToLocal(folderWithTadsOnUnix);
		listOfXmlObjects = getTaMessages(localPath, results);
		mapOfXmlObjects = getCompareMapOfXmlObjects();
		countOfEnrichedTads = verifyEnrichmentPRDS(jsonForTest, mapOfXmlObjects, productPRDSmapping, results);
		results.put("All files was enriched", verifyThatAllFilesWasEnriched(mapOfXmlObjects, countOfEnrichedTads));
		updateReportWithTestResults(results);
		updateReportToFinal(reportFile, results, startTime);
	}
	
	@Test
	public void productEnrichmentFinancialProductType(){
		Map<String, String> results = new LinkedHashMap<String, String>();
		folderWithTadsOnUnix = props.getProperty("TADSFOLDERONUNIX");
		productFinancialMapping = getMapXLSdata(testDataFolder+"FinancialProductTypeMapping_LDN.xlsx");
		downloadFromUnixToLocal(folderWithTadsOnUnix);
		listOfXmlObjects = getTaMessages(localPath, results);
		mapOfXmlObjects = getCompareMapOfXmlObjects();
		countOfEnrichedTads = verifyEnrichmentFinancial(jsonForTest, mapOfXmlObjects, productFinancialMapping, results);
		results.put("All files was enriched", verifyThatAllFilesWasEnriched(mapOfXmlObjects, countOfEnrichedTads));
		updateReportWithTestResults(results);
		updateReportToFinal(reportFile, results, startTime);
	}
	
	@Test
	public void cashflowEnrichmentTest(){
		Map<String, String> results = new LinkedHashMap<String, String>();
		folderWithTadsOnUnix = props.getProperty("TADSFOLDERONUNIX");
		downloadFromUnixToLocal(folderWithTadsOnUnix);
		listOfXmlObjects = getTaMessages(localPath, results);
		mapOfXmlObjects = getCompareMapOfXmlObjects();
		
		TaMessage pv1 =  mapOfXmlObjects.get(jsonForTest.getTraderBook().get(0).getTrades().get(0).getTradeId());
		
		if(pv1.getValuation().getFeature().get(0).getCashflow().getCashFlowName().equals("UNREALISEDPL")){
			results.put("The cash_flow_name attribute is populated with \"UNREALISED\" value. ", "PASS");
		} else {
			results.put("The cash_flow_name attribute is populated with "+pv1.getValuation().getFeature().get(0).getCashflow().getCashFlowName()+" value" , "FAIL");
		}
		cleanLocalTempDirectory(localPath);
		downloadFromUnixToLocalForFiltering(folderWithTadsOnUnix);
		listOfXmlObjects = getTaMessages(localPath, results);
		mapOfXmlObjects = getCompareMapOfXmlObjects();
		ensureThatFileWasFiltered(results, 1);
		updateReportWithTestResults(results);
		updateReportToFinal(reportFile, results, startTime);
	}
	
	@Test
	public void counterPartyEnrichmentTest() {
		Map<String, String> results = new LinkedHashMap<String, String>();
		folderWithTadsOnUnix = props.getProperty("TADSFOLDERONUNIX");
		downloadFromUnixToLocal(folderWithTadsOnUnix);
		listOfXmlObjects = getTaMessages(localPath, results);
		mapOfXmlObjects = getCompareMapOfXmlObjects();
		
		for (TaMessage pv : mapOfXmlObjects.values()){
			verifyXMLValueAgainstData("The type attribute is populated with \"L_CPTY_ID\" value for "+pv.getValuation().getAccountableId(), 
					pv.getValuation().getCounterParty().getType() , "L_CPTY_ID", results);
			verifyThatEntityNotExists("The organization_unit_id attribute is absent", pv.getValuation().getCounterParty().getOrganizationUnitId(), results);
			verifyThatEntityNotExists("The organization_unit_type attribute is absent", pv.getValuation().getCounterParty().getOrganizationUnitType(), results);
			verifyThatEntityNotExists("The organization_unit_source_system  attribute is absent.", pv.getValuation().getCounterParty().getOrganizationUnitSourceSystem(), results);
		}
	}

}
