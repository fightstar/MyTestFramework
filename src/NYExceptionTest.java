

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
/**
 * Class that realizes classic assertion-style test approach for validate Exceptions.
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
public class NYExceptionTest extends TestHelper {
	
	//variables
			public static final String COMMONTESTFOLDER = "/05_Exceptions/";
			public static String localPath;
			private static ArrayList<String> listOfJsonDatasets = new ArrayList<String>();
			ArrayList<PnLReport> listWithJsonsObjects;
			Map<String, PnLReport> mapOfJsons = new HashMap<String, PnLReport>();
			private static Map <String, String> mapOfWorkflowId;
			private long totalSuiteDurationStart;
			
			public String folderWithTadsOnUnix;
			String testDataFolder;
			
			
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
//				cleanLocalTempDirectory(localPath);
				unixFolderPath = props.getProperty("UNIX_PATH_DATA");
				props.put(
						"SCRIPTS_FOLDER",
						"C:\\Users\\shevvla\\SVN\\Automation_Luxoft\\TA\\TestData\\DS\\PnL_Credit\\Scripts");
				ScriptsPath = props.getProperty("SCRIPTS_FOLDER");
				// report
				BUSINESS_LINE = props.getProperty("BUSINESS_LINE");
				Date reportDate = new Date();
				reportFile = new Report(".\\Reports\\" + BUSINESS_LINE + "_"
						+ DateFormat.format(reportDate)+"_exceptions" + ".xlsx");
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
							/*TemplateActions.updateReport(reportFile, "Json files", "PASS",
									"json file " + jsonFilePathToCopy
											+ " was uploaded");*/
	      					result += uploadToUnix(fileTestName, jsonFilePathToCopy);
							uploadToHadoop();
							props.put("FEEDID", "18"); // 7 LONDON 18 NY
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
			public void testDataPreparationStep(String fileName, String testFolderName) {
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
			public void noExceptionWithEmptyTradeVersion() {
				Common.print("noExceptionWithEmptyTradeVersion test started");
				Map<String, String> results = new LinkedHashMap<String, String>();
				folderWithTadsOnUnix = props.getProperty("TADSFOLDERONUNIX");
				downloadFromUnixToLocal(folderWithTadsOnUnix);
				listOfXmlObjects = getTaMessages(localPath, results);
				mapOfXmlObjects = getCompareMapOfXmlObjects();
				
				ensureThatFileWasNotFiltered(results, 3);
				
				updateReportWithTestResults(results);
				updateReportToFinal(reportFile, results, startTime);
				Common.print("noExceptionWithEmptyTradeVersion test finished");
				
			}
			
			@Test
			public void tbeOnBlankProductAttributes(){
				Common.print("tbeOnBlankProductAttributes test started");
				Map<String, String> results = new LinkedHashMap<String, String>();
				folderWithTadsOnUnix = props.getProperty("TADSFOLDERONUNIX");
				downloadFromUnixToLocalForFiltering(folderWithTadsOnUnix);
				listOfXmlObjects = getTaMessages(localPath, results);
				mapOfXmlObjects = getCompareMapOfXmlObjects();
				ProcessStatus processStatus = listOfXmlObjects.get(0).getProcessStatus();
				
				
				verifyXMLValueAgainstData("Ensure that processed dataset was discarded on blank FO Product Type", processStatus.getExceptionList().getException().get(0).getDescription(), "Field is blank: front_office_product_type", results);
				updateReportWithTestResults(results);
				updateReportToFinal(reportFile, results, startTime);
				Common.print("tbeOnBlankProductAttributes test finished");
			}
}
