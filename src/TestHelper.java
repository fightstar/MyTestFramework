package DataSourcingPack.PnL_Credit;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import DataSourcingPack.PnL_Credit.FilterConfigClasses.FiltersType;
import DataSourcingPack.PnL_Credit.JsonObjectClasses.PnLReport;
import DataSourcingPack.PnL_Credit.JsonObjectClasses.Trades;
import DataSourcingPack.PnL_Credit.StaticDataClasses.RDBook.BOOK;
import UtilsPack.Common;
import UtilsPack.Report;
import UtilsPack.TemplateActions;
import UtilsPack.TimeCounter;
import UtilsPack.UtilsRemoteHost;
import UtilsPack.UtilsSQL;
import UtilsPack.UtilsXLS;
import UtilsPack.VariableStorage;

public class TestHelper {

	protected static final SimpleDateFormat DateFormat = new SimpleDateFormat(
			"yyyyMMdd_HHmmss");
	public static Properties props = new Properties();
	
	private static final int waitTime = 600000;


	public UtilsRemoteHost rh = null;
	public VariableStorage testVariables = new VariableStorage();
	public static Date date;
	public static Object json;
	public static String unixFolderPath = null;
	public static String basicPath = null;
	public String dataProvFile = null;
	public List<String> jsonSourcesFilesWithPath = null;
	public String jsonFilePathToCopy = null;
	String fileTestName = null;
	public static String ScriptsPath = null;
	public InputStream xml = null;
	public InputStream xsd = null;
	public static Report reportFile;
	static long startTime;
	long endTime;
	public static String TestName = null;
	public static String BUSINESS_LINE = null;
	PnLReport jsonForTest;

	public static String localPath;
	public TaMessage taMessage;
	public FiltersType filters;
	public List<TaMessage> listOfXmlObjects;
	public Map<String, TaMessage> mapOfXmlObjects;
	StringBuilder builder;
	protected void addToReport(String first, String second, String third, Report reportfile){
		ArrayList<String> strings = new ArrayList<String>() {};
		strings.add(0,first); 
		strings.add(1,second);
		strings.add(2,third);
		reportfile.add(strings);
	}

	/**
	 * 
	 * @author vshevchenko
	 * @return map of TaMessages compared with json by tradeID
	 */
	protected Map<String, TaMessage> getCompareMapOfXmlObjects() {
		Map<String, TaMessage> compareMap = new HashMap<String, TaMessage>();
		for (TaMessage tamessage : listOfXmlObjects) { // taMessage
			for (Trades trade : jsonForTest.getTraderBook().get(0).getTrades()) {
				String[] splittedAccountableId = tamessage.getValuation().getAccountableId().split("/");
				//System.out.println("splittedAccountableId ===> "+ splittedAccountableId.length);
				String accId ="";
				if(props.get("FEEDID").equals("18")){
					accId = splittedAccountableId[4];
				} else if(props.get("FEEDID").equals("7")){
					accId = splittedAccountableId[5];
				}
//				String accId = splittedAccountableId[5]; //? verify that in NY it is 5 not 4 
				if (accId.equals(trade.getTradeId())) {
					compareMap.put(trade.getTradeId(), tamessage);
				}
			}
		}
		return compareMap;
	}

	/**
	 * Delete folders created on Unix
	 */
	protected void deleteDirOnUnix() {
		UtilsRemoteHost rh = new UtilsRemoteHost(props.get("HDFS_USER")
				.toString(), props.get("HDFS_PASS").toString(), props.get(
				"HDFS_URL").toString());
		String commandToRunForClean = "cd %%UNIX_PATH_DATA%%; rm -r %%TADSFOLDERONUNIX%%; ls;";
		Common.print("After method : file was deleted");
		rh.shellExecutorViaExecChannel(substitute(commandToRunForClean));
	}

	private String makeTaMessageString(String xmlString) {
		StringBuilder sb = new StringBuilder();
		sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?> ");
		sb.append("\n");
		sb.append("<ta_message");
		String s1 = StringUtils.substringBetween(xmlString, "<ta_message",
				"</ta_message>");
		sb.append(s1);
		sb.append("</ta_message>");
		return sb.toString();
	}

	/**
	 * Get .xml files from folder used regex mask
	 * 
	 * @param folder
	 * @return list of files
	 */
	protected List<String> getXmLFilesFromLocalDir(String folder) {
		String regex_mask = ".*.xml";
		return Common.listFilesForFolder(new File(folder), regex_mask);
	}
/*
	*//**
	 * OLD APPROACH
	 * Parse file located on "pathToFile" in TaMessage object
	 * 
	 * @param pathToFile
	 * @param reportFile
	 * @return TaMessage object
	 *//*
	public TaMessage xmlParsingInObject(String pathToFile) {
		File xmlFile = new File(pathToFile);
		JAXBContext jaxbContext = null;
		try {
			jaxbContext = JAXBContext.newInstance(TaMessage.class);
		} catch (JAXBException e) {
			e.printStackTrace();
		}
		Unmarshaller jaxbUnmarshaller = null;
		try {
			jaxbUnmarshaller = jaxbContext.createUnmarshaller();
		} catch (JAXBException e) {
			e.printStackTrace();
		}
		try {
			taMessage = (TaMessage) jaxbUnmarshaller.unmarshal(xmlFile);
			Common.print("Xml is parsed successfully from file " + pathToFile
							+ ", object is created");
		} catch (JAXBException e) {
			e.printStackTrace();
			Common.print(pathToFile + " xml isn't parsed");
		}
		return taMessage;
	}*/
	/**
	 * Parse string to TaMessage object
	 * @param stringToParse
	 * @return taMessage object
	 */
	public TaMessage xmlParsingInObject(String stringToParse){
		
		JAXBContext jaxbContext = null;
		try {
			jaxbContext = JAXBContext.newInstance(TaMessage.class);
		} catch (JAXBException e) {
			e.printStackTrace();
		}
		Unmarshaller jaxbUnmarshaller = null;
		try {
			jaxbUnmarshaller = jaxbContext.createUnmarshaller();
		} catch (JAXBException e) {
			e.printStackTrace();
		}
		StringReader reader  = new StringReader(stringToParse);
		
		
		try {
			taMessage = (TaMessage) jaxbUnmarshaller.unmarshal(reader);
		} catch (JAXBException e) {
			e.printStackTrace();
		}
		
		
		return taMessage;
		
	}
	public FiltersType xmlParsinginFiltersObjects(String pathToFile){
		File xmlFile = new File(pathToFile);
		JAXBContext jaxbContext = null;
		try {
			jaxbContext = JAXBContext.newInstance(FiltersType.class);
		} catch (JAXBException e) {
			e.printStackTrace();
		}
		Unmarshaller jaxbUnmarshaller = null;
		try {
			jaxbUnmarshaller = jaxbContext.createUnmarshaller();
		} catch (JAXBException e) {
			e.printStackTrace();
		}
		try {
			filters = (FiltersType) jaxbUnmarshaller.unmarshal(xmlFile);
			/*TemplateActions.updateReport(reportFile, "XML parsing", "PASS",
					"xml is parsed successfully from file " + pathToFile
							+ ", object is created");*/
		} catch (JAXBException e) {
			e.printStackTrace();
			Common.print(e);
			/*TemplateActions.updateReport(reportFile, "XML parsing", "FAIL",
					"xml isn't parsed");*/
		}
		return filters;
	}
	public Map<String, String> getMapOfWorflowIdAndDataSets(List<String> datasets){
		Map <String, String> map  = new HashMap<>();
		for(String dataset : datasets){
			props.put("JSON_DATASET", dataset);
			map.put(dataset, getWorkflowId().substring(0, 36));
		}
		return map;
	}
	/**
	 * Get workflow id from data base Uses %JSON_DATASET% variable
	 * 
	 * @return worlflowId string
	 */
	public String getWorkflowId() {
		String fileName = ScriptsPath + "\\getWorkflowId.sql";
		String getWorkflowIdQuery = TemplateActions.read_file(fileName);
		Common.print(substitute(getWorkflowIdQuery));
		return UtilsSQL.getSQLvalue(substitute(getWorkflowIdQuery));
	}
	/**
	 * Get DeliveryUnitInstanceId from data base Uses %JSON_DATASET% variable
	 * 
	 * @return DeliveryUnitInstanceId string
	 */
	public String getDeliveryUnitInstanceIdFromDataBase(){
		String fileName = ScriptsPath + "\\selectDeliveryUnitInstanceId.sql";
		String getDeliveryUnitInstanceIdQuery = TemplateActions.read_file(fileName);
		Common.print("getDeliveryUnitInstanceIdQuery :" +substitute(getDeliveryUnitInstanceIdQuery));
		return UtilsSQL.getSQLvalue(substitute(getDeliveryUnitInstanceIdQuery));
	}
	/**
	 * Get concatenated accountable id from .json file for NY
	 * @author shevvla
	 * @param jsonForTest
	 * @param tradenumber
	 * @return String value of accountable id
	 */
	public String  getAccountableIdFromJsonForNY(PnLReport jsonForTest, int tradenumber){
		int i = tradenumber - 1;
		StringBuilder accountableId = new StringBuilder();
		if(jsonForTest.getTraderBook().get(0).getTrades().get(i).getPRDSProductType().equals("")){
		accountableId.append("/");	
		accountableId.append(jsonForTest.getTraderBook().get(0).getTraderBookCode());
		accountableId.append("/");
		accountableId.append(jsonForTest.getTraderBook().get(0).getTrades().get(i).getFinancialProductTypeId());
		accountableId.append("/");
		accountableId.append(jsonForTest.getTraderBook().get(0).getTrades().get(i).getPartyId());
		accountableId.append("/");
		accountableId.append(jsonForTest.getTraderBook().get(0).getTrades().get(i).getTradeId());
		
		} else
			accountableId.append("/");		
			accountableId.append(jsonForTest.getTraderBook().get(0).getTraderBookCode());
			accountableId.append("/");
			accountableId.append(jsonForTest.getTraderBook().get(0).getTrades().get(i).getPRDSProductType());
			accountableId.append("/");
			accountableId.append(jsonForTest.getTraderBook().get(0).getTrades().get(i).getPartyId());
			accountableId.append("/");
			accountableId.append(jsonForTest.getTraderBook().get(0).getTrades().get(i).getTradeId());
			
		return accountableId.toString();
	}
	
	public String getAccountableIdFromJsonForLondon(PnLReport jsonForTest, int tradenumber) {
		int i = tradenumber - 1;
		StringBuilder accountableId = new StringBuilder();
		
		if(jsonForTest.getTraderBook().get(0).getTrades().get(i).getPRDSProductType().equals("")){
			accountableId.append("/");	
			accountableId.append(jsonForTest.getTraderBook().get(0).getTraderBookCode());
			accountableId.append("/");
			accountableId.append(jsonForTest.getTraderBook().get(0).getTraderBookSourceSystemName());
			accountableId.append("/");
			accountableId.append(jsonForTest.getTraderBook().get(0).getTrades().get(i).getFinancialProductTypeId());
			accountableId.append("/");
			accountableId.append(jsonForTest.getTraderBook().get(0).getTrades().get(i).getPartyId());
			accountableId.append("/");
			accountableId.append(jsonForTest.getTraderBook().get(0).getTrades().get(i).getTradeId());
			
			} else
				accountableId.append("/");		
				accountableId.append(jsonForTest.getTraderBook().get(0).getTraderBookCode());
				accountableId.append("/");
				accountableId.append(jsonForTest.getTraderBook().get(0).getTraderBookSourceSystemName());
				accountableId.append("/");
				accountableId.append(jsonForTest.getTraderBook().get(0).getTrades().get(i).getPRDSProductType());
				accountableId.append("/");
				accountableId.append(jsonForTest.getTraderBook().get(0).getTrades().get(i).getPartyId());
				accountableId.append("/");
				accountableId.append(jsonForTest.getTraderBook().get(0).getTrades().get(i).getTradeId());
		
		
		
		return accountableId.toString();
		
	}
	/**
	 * Read file to string
	 * 
	 * @param path
	 * @param encoding
	 * @return string
	 * @throws IOException
	 */
	public String readFile(String path, Charset encoding) throws IOException {
		byte[] encoded = Files.readAllBytes(Paths.get(path));
		return new String(encoded, encoding);
	}

	/**
	 * Create .xml file from string
	 * 
	 * @param string
	 * @param fileName
	 * @return file
	 */
	public File createXmlFileFromString(String string, String fileName) {
		File file = new File(fileName);
		try {
			FileUtils.writeStringToFile(file, string);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return file;
	}

	/**
	 * Returns map of results which was supplemented by parameters
	 * 
	 * @param stepName
	 * @param xmlValue
	 * @param expectedValue
	 * @param results
	 * @return Map<step name, step result>
	 *
	 */
	protected Map<String, String> verifyXMLValueAgainstData(String stepName, String xmlValue,
			String expectedValue, Map<String, String> results) {
				results.put(stepName, TestBase.assertEquals(xmlValue, expectedValue));
				if (results.entrySet().toString().contains("FAIL")) {
					Common.print(TestBase.getVerificationFailures());
					results.put(stepName, TestBase.getVerificationFailure());
				}
				return results;
			
			}

	/**
	 * Update report file with values from map of results
	 * 
	 * @param results
	 */
	public void updateReportWithTestResults(Map<String, String> results) {
		for (Map.Entry<String, String> entry : results.entrySet()) {
			if (entry.getValue().contains("AssertionError")) {
				TemplateActions.updateReport(reportFile, entry.getKey(),
						"FAIL", entry.getValue());
			} else
				TemplateActions.updateReport(reportFile, entry.getKey(),
						entry.getValue(), "");
		}
	}

	/**
	 * Update(final) of report file with duration of test
	 * 
	 * @param reportFile
	 * @param results
	 * @param startTime
	 *            -of test
	 */
	protected void updateReportToFinal(Report reportFile, Map<String, String> results, long startTime) {
		String status = "";
		if (results.entrySet().toString().contains("FAIL")
				| results.entrySet().toString().contains("AssertionError")) {
			status = "FAIL";
		} else
			status = "PASS";
		reportFile.addFinal(status, startTime);
		endTime = TimeCounter.getCurrentTime();
		TimeCounter.getDifference(endTime - startTime);
		reportFile.add(Arrays.asList(TimeCounter.getDifference(endTime
				- startTime)));
	
	}

	/**
	 * Clean incoming string and return valid string for parsing
	 * 
	 * @param tadsPathForTest
	 * @return
	 */
	public String getXMLStringFromDownloadedFile(String tadsPathForTest) {
	
		String stringFromRawXMLFile = null;
		try {
			stringFromRawXMLFile = readFile(tadsPathForTest,
					StandardCharsets.UTF_8);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return stringFromRawXMLFile;
	}

	/**
	 * Get BOOK record from service request to RD_BOOK
	 * 
	 * @param traderBookCode
	 * @param traderBookSystemName
	 * @return string that contains BOOK
	 */
	public String getStaticRecordFromServiceRequest(String traderBookCode, String traderBookSystemName) {
		long duration = Long.parseLong("10000");
		long startTime = System.currentTimeMillis(); // fetch starting time
		props.put("TRADERBOOKCODE", traderBookCode);
		props.put("TRADERBOOKSOURCESYSTEMNAME", traderBookSystemName);
	
		String result = "";
		rh = new UtilsRemoteHost(props.getProperty("HDFS_USER"),
				props.getProperty("HDFS_PASS"), props.getProperty("HDFS_URL"));
	
		String fileName = props.getProperty("PATH")
				+ "\\getRecordFromRD_BOOKStatic.sh";
		Common.print(fileName);
		String curlForStaticData = TemplateActions.read_file(fileName);
		Common.print(Common.replaceGlobalTags(curlForStaticData, props));
		String command = Common.replaceGlobalTags(curlForStaticData, props);
		result = rh.shellExecutorViaExecChannel(command);
	
		/*
		 * while ((System.currentTimeMillis() - startTime) < duration) { result
		 * = rh.shellExecutor(command); try { Thread.sleep(5000); } catch
		 * (InterruptedException e) { e.printStackTrace(); } }
		 */
	
		if (!result.contains("exit-status: 0"))
			result = "exit-status: 1";
		Common.print("service request result: " + result);
		try{
		result = result.substring(result.indexOf("<BOOK>"),
				result.indexOf("</BOOK>"))
				+ "</BOOK>";
		}catch(StringIndexOutOfBoundsException e){
			e.printStackTrace();
			result = e.getStackTrace()+e.getMessage();
		}
		return result;
	}

	/**
	 * Upload file to Unix
	 * 
	 * @param fileName
	 *            specify file that you want to upload
	 */
	public String uploadToUnix(String fileName, String jsonFilePathToCopy) {
	
		String commandToRunForClean = "cd %%UNIX_PATH_DATA%%; rm *.json; ls; ";
	
		rh = new UtilsRemoteHost(props.getProperty("HDFS_USER"),
				props.getProperty("HDFS_PASS"), props.getProperty("HDFS_URL"));
		rh.shellExecutorViaExecChannel(substitute(commandToRunForClean));
	
		return rh.FileCopyTo(props.getProperty("UNIX_PATH_DATA"), jsonFilePathToCopy,
				fileName);
		
	}

	/**
	 * Download from Unix to local
	 * 
	 * @param dataFolderOnUnix
	 */
	public String downloadFromUnixToLocal(String dataFolderOnUnix) {
		String localPath = basicPath + "\\temp";
		String result = "";
		UtilsRemoteHost rh = new UtilsRemoteHost(props.get("HDFS_USER")
				.toString(), props.get("HDFS_PASS").toString(), props.get(
				"HDFS_URL").toString());
	
		//props.put("DATA_FOLDER_NAME", dataFolderOnUnix);
	
		result  = rh.FileCopyFrom(localPath + "\\", props.getProperty("UNIX_PATH_DATA")
				+ "/" + dataFolderOnUnix + "/TADS_Files/SRV_VAL/data/*");
		//test approach
		/*result  = rh.FileCopyFrom(localPath + "\\", props.getProperty("UNIX_PATH_DATA")
				+ "/" + dataFolderOnUnix + "/TADS_Files/SRV_FILTER/discarded/*");*/
		
		rh.closeSession();
		return result;
	
	}
	/**
	 * Get files from Unix folders SRV_VAL/discarded/  SRV_FIL/discarded/  SRV_TRANS/discarded/  to local folder
	 * @author shevvla
	 * @param dataFolderOnUnix
	 */
	public void downloadFromUnixToLocalForFiltering(String dataFolderOnUnix){
		String localPath = basicPath + "\\temp";
		String result = "";
		UtilsRemoteHost rh = new UtilsRemoteHost(props.get("HDFS_USER")
				.toString(), props.get("HDFS_PASS").toString(), props.get(
				"HDFS_URL").toString());
		
		props.put("DATA_FOLDER_NAME", dataFolderOnUnix);
		
		rh.FileCopyFrom(localPath + "\\", props.getProperty("UNIX_PATH_DATA")
				+ "/" + dataFolderOnUnix + "/TADS_Files/SRV_VAL/discarded/*"); 
		
		rh.FileCopyFrom(localPath + "\\", props.getProperty("UNIX_PATH_DATA")
				+ "/" + dataFolderOnUnix + "/TADS_Files/SRV_FILTER/discarded/*");
		rh.FileCopyFrom(localPath + "\\", props.getProperty("UNIX_PATH_DATA")
				+ "/" + dataFolderOnUnix + "/TADS_Files/SRV_TRANS/discarded/*");
		
		
	}

	public String getTADSFromHadoop() {
		String tamessage = "";
		rh = new UtilsRemoteHost(props.get("HDFS_USER").toString(), props.get(
				"HDFS_PASS").toString(), props.get("HDFS_URL").toString());
		String fileName = ScriptsPath + "\\getSRV_FILPUBFolderNumber.sql";
		String query = TemplateActions.read_file(fileName);
		String folderNumber = UtilsSQL.getSQLvalue(substitute(query));
		props.put("SRV_FILPUB_FOLDER", folderNumber);
		String batFilename = ScriptsPath + "\\getTadsFromSRV_FILPUB.bat";
		String command = TemplateActions.read_file(batFilename);
		tamessage = rh.shellExecutorViaExecChannel(substitute(command));
	
		return tamessage;
	}
	public String getFilterConfigurationXMLFromHadoop(){
		rh = new UtilsRemoteHost(props.get("HDFS_USER").toString(), props.get(
				"HDFS_PASS").toString(), props.get("HDFS_URL").toString());
		StringBuilder command = new StringBuilder();
		command.append("cd %%UNIX_PATH_DATA%%;");
		command.append("hadoop fs -get %%RM_ENV%%/fimta/app/xsl/credit/ods/filter/credit_ods_filters_ny.xml");
		String commandToExec = substitute(command.toString());
		return rh.shellExecutorViaExecChannel(commandToExec);
		
	}
	/**
	 * Upload files from Unix to Hadoop
	 */
	public String uploadToHadoop() {
		String fileName = ScriptsPath + "\\fileCopyFromUnixToHadoop.bat";
		String hadoopUploadCommand = TemplateActions.read_file(fileName);
		rh = new UtilsRemoteHost(props.getProperty("HDFS_USER"),
				props.getProperty("HDFS_PASS"), props.getProperty("HDFS_URL"));
		return rh.shellExecutorViaExecChannel(substitute(hadoopUploadCommand));
	}
	
	public String uploadToHadoopBulkAndInsertInDb(String testFolder){
		String result = "";
		String commandToRunForClean = "cd %%UNIX_PATH_DATA%%; rm *.json; ls; ";
		/*String feed_inst_query = TemplateActions.read_file(ScriptsPath+"\\insertRecordToDB.sql");
		String temp = feed_inst_query;*/
		date = new Date();
		// get filenames
		List<String> jsonSourcesFilesWithPath = getJsonFileInDir(basicPath+testFolder);
		Common.print(jsonSourcesFilesWithPath.size());		
		
		for (String jsonSource : jsonSourcesFilesWithPath){
			UtilsSQL.getDBConnection(props);
			fileTestName = Common.updateFileName(Common.getFileNameFromAbsPath(jsonSource),"_" + DateFormat.format(date));
			props.put("JSON_DATASET", fileTestName);
			jsonFilePathToCopy = jsonSourcesFilesWithPath.get(jsonSourcesFilesWithPath.indexOf(jsonSource));
			result += uploadToUnix(fileTestName, jsonFilePathToCopy);
			props.put("FEEDID", "18"); // 7 LONDON 18 NY
			props.put("ORGLSRCID", "CREDIT");
			props.put("FEEDENTITYID", jsonForTest.getTraderBook().get(0)
					.getTraderBookCode());
			result += executeInsert();
			try {
				Thread.sleep(400000); //6min
			} catch (InterruptedException e) {
				Common.print("InterruptedException in waiting for processing");
			}
			
		}

		return result;
		
	}

	/**
	 * Run GetVAluations.sh script to get .xml files
	 * 
	 * @param workflowId
	 */
	public void runGetValuations(String workflowId) {
		props.put("WORKFLOW_ID", workflowId);
		String fileName = ScriptsPath + "\\runGetValuations.bat";
		String command = TemplateActions.read_file(fileName);
		rh = new UtilsRemoteHost(props.getProperty("HDFS_USER"),
				props.getProperty("HDFS_PASS"), props.getProperty("HDFS_URL"));
		rh.shellExecutorViaExecChannel(substitute(command));
	
	}
	
	public void createFolderAndgetTADSfromHDFS() {
		String fileName = ScriptsPath + "\\createFLDRonUnixAndGetTADSfromHDFS.bat";
		String command = TemplateActions.read_file(fileName);
		rh = new UtilsRemoteHost(props.getProperty("HDFS_USER"),
				props.getProperty("HDFS_PASS"), props.getProperty("HDFS_URL"));
		rh.shellExecutorViaExecChannel(substitute(command));
	}
	/**
	 * Insert record to FEED_INSTC table Initiate Tibco processing
	 * 
	 * @return
	 */
	public String executeInsert() {
		String result = "";
		String fileName = ScriptsPath + "\\insertRecordToDB.sql";
		String feed_inst_query = TemplateActions.read_file(fileName);
		result += UtilsSQL.executeSQLStatement(substitute(feed_inst_query));
		return result;
	}

	/**
	 * Waits until status will change from 1200(processing) to another This
	 * method to prevent processing bunch of files in one workflow
	 * 
	 */
	public void waitForTibcoProcessingBegin() {
		String fileName = ScriptsPath
				+ "\\countRowsAgainstStatus1200FromFEED_INSTC.sql";
		String waitForStatusQuery = TemplateActions.read_file(fileName);
		System.out.println(substitute(waitForStatusQuery));
		System.out
				.println(UtilsSQL.getSQLvalue(substitute(waitForStatusQuery)));
		while (UtilsSQL.getSQLvalue(substitute(waitForStatusQuery)).equals("1")) {
			Common.sleep(5000);
		}
	
	}

	/**
	 * Waits for dataset
	 * 
	 * @return
	 */
	public String waitingForProcessingDataSet() {
		String waitProcessingResult = "";
		String query = Common.readFileToString(ScriptsPath
				+ "\\waitForProcessingDataset.sql");
		Common.print("query: " + query);
		query = Common.replaceGlobalTags(query, props);
		Common.print("query after replacement of global tags: " + query);
		waitProcessingResult = TemplateActions.waitProcessingPnL(query, props);
		return waitProcessingResult;
	
	}
	
	public String waitForAllDatasetsProcessed() {
		String waitProcessingResult = "";
		String query = Common.readFileToString(ScriptsPath
				+ "\\decodeFeedInstcStusIs1500.sql");
		query = Common.replaceGlobalTags(query, props);
		Common.print("after replacement of global tags: " + query);
		waitProcessingResult = waitForProcessingFile(query);
		return waitProcessingResult;
	}

	public String waitForProcessingFile(String query) {
		boolean sqlResult = Boolean.parseBoolean(UtilsSQL.getSQLvalue(query));
		int counter = 1;
		do {
			if (sqlResult){
				return QueryResult.PASSED.getTitle();
			} else {
				try {
					Thread.sleep(waitTime);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				sqlResult = Boolean.parseBoolean(UtilsSQL.getSQLvalue(query));
				counter++;
			}
		} while (!sqlResult && counter < 10);
		return QueryResult.FAILED.getTitle();
	}
	
	 public String getHDFSfolderName(){
		String sql_result = "";
		String query = Common.readFileToString(ScriptsPath
				+ "\\getHDFS_FLDR_NAME.sql");
		Common.print("query: " + query);
		query = Common.replaceGlobalTags(query, props);
		Common.print("query after replacement of global tags: " + query);
		sql_result = UtilsSQL.getSQLvalue(query);
		return sql_result;
	}
	 
	/**
	 * Creates BOOK object from .xml file
	 * 
	 * @param xml
	 *            - file
	 * @return Book object
	 */
	public BOOK createObjectFromStaticData(File xml) {
		BOOK bookFromStaticSystem = null;
		try {
			JAXBContext jaxbContext = JAXBContext.newInstance(BOOK.class);
	
			Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
			bookFromStaticSystem = (BOOK) jaxbUnmarshaller.unmarshal(xml);
			return bookFromStaticSystem;
		} catch (JAXBException e) {
			e.printStackTrace();
		}
		return bookFromStaticSystem;
	
	}

	/**
	 * Get parsed xml TaMessage objects from files on local
	 * 
	 * @param localPath
	 *            - path to temp folder on local
	 * @param results
	 * @return list of TaMessage objects
	 */
	protected List<TaMessage> getTaMessages(String localPath, Map<String, String> results) {
		List<String> filesToTest = getXmLFilesFromLocalDir(localPath);
		if (filesToTest.isEmpty()){
			results.put("No files to test", "FAIL");
		}
	
		List<TaMessage> listOfObjects = new ArrayList<>();
		for (String file : filesToTest) {
	
			String xmlString = getXMLStringFromDownloadedFile(file);
			//new approach using regexp
			List<String> taMessages = new ArrayList<>();
			//Pattern pattern = Pattern.compile("(<ta_message)([\\s\\S]*|[^<]*)(</ta_message>)");
			Pattern pattern = Pattern.compile("(<ta_message.+?>)(.+?)(<\\/ta_message>)", Pattern.DOTALL);
		    Matcher matcher = pattern.matcher(xmlString.toString());
		    
			while(matcher.find()) {
				StringBuilder tamessage = new StringBuilder();
				taMessages.add(
						tamessage
						.append(matcher.group(1))
						.append(matcher.group(2))
						.append(matcher.group(3))
						.toString()
						);
			}
			
			for (String taMessage : taMessages){
				//TODO (done) create method xmlParsingobject that takes String in param and delete step with create file from string
				//createXmlFileFromString(taMessage, file); //need to be deleted
				listOfObjects.add(xmlParsingInObject(taMessage));
			}
			//old approach
			/*count = StringUtils.countMatches(xmlString, findString); //count of ta_message entries in xml file
			List<String> objectsToParse = new ArrayList<String>();
			if (count == 1) {
				Common.print("count of tads = 1");
				createXmlFileFromString(xmlString, file);
				listOfObjects.add(xmlParsingInObject(file));
			} else {
				Common.print("count of tads = " + count);
				int startIndex = xmlString.indexOf(findString);
				xmlString = xmlString.substring(startIndex);
				String[] splittedXml = xmlString.split(findString);
				Common.print("Size of splittedXml: " + splittedXml.length); //-1
				for (String oneStringObject : splittedXml) {
					if (oneStringObject.length() > 0) {
						String closingTab = "</ta_message>";
						oneStringObject = oneStringObject.split(closingTab)[0];
						String objectToParse = findString + oneStringObject
								+ closingTab;
						objectsToParse.add(objectToParse);
					}
				}
				Common.print("Number of strings to parse: "
						+ objectsToParse.size());
				for (String stringObject : objectsToParse) {
					createXmlFileFromString(stringObject, file);
					listOfObjects.add(xmlParsingInObject(file));
				}
	
			}*/
		
		}
	
		return listOfObjects;
	
	}

	protected Map<String, String> verifyFilteringAmount(PnLReport jsonForTest,
			Map<String, TaMessage> mapOfXmlObjects, Map<String, String> results) {
		
		
		for(Entry entry: mapOfXmlObjects.entrySet()){
			for(Trades trade : jsonForTest.getTraderBook().get(0).getTrades()){
				if(entry.getKey().equals(trade.getTradeId()) ){
					
					
					Common.print(trade.getTradeLegs().get(0).getMeasures().get(0).getReportableAmountClass());
					Common.print(trade.getTradeLegs().get(0).getMeasures().get(1).getReportableAmountClass());
					
					//case 1
					if(trade.getTradeLegs().get(0).getMeasures()
							.get(0).getReportableAmountClass()!="Balance Sheet" || trade.getTradeLegs().get(0).getMeasures()
							.get(1).getReportableAmountClass()!="Balance Sheet"){
						results.put("trade with tradeId "+trade.getTradeId() +" was filtered", "PASS");
						
						StringBuilder status = getProcessStatus(
								mapOfXmlObjects, entry);
//						results.put(mapOfXmlObjects.get(entry.getKey()).getProcessStatus(), "blblb");
						results.put(status.toString(), "PASS");
						
						System.out.println("");
					}
				
					
				}
			}
		}
			
			
		return results;
	}
	protected Map<String, String> verifyFilteringReportableAmount(PnLReport jsonForTest,
			Map<String, TaMessage> mapOfXmlObjects, Map<String, String> results, boolean flag){
		String outOfScope = "PV MTM";
		String attributValue = null;
		List<DataSourcingPack.PnL_Credit.JsonObjectClasses.Measures> measures = new ArrayList<>();
		
		for(Entry entry: mapOfXmlObjects.entrySet()){
			 for(Trades trade : jsonForTest.getTraderBook().get(0).getTrades()){
				 if(entry.getKey().equals(trade.getTradeId())){ // get correspondent Trade object
					 if(flag){ //processed
						int numberOfLeg = trade.getTradeLegs().size();
						if(numberOfLeg  == 1){
							measures = trade.getTradeLegs().get(0).getMeasures();
							for ( DataSourcingPack.PnL_Credit.JsonObjectClasses.Measures measure : measures){
								if (measure.getMeasureStandardName().equals(outOfScope) && measure.getCurrencyType().equals("TXC")){
									results.put("Check generated TADS-file, ensure that " + trade.getTradeLegs().get(0).getTradeLegID()+" was not filtered", "PASS");
									Common.print("Check generated TADS-file, ensure that " + trade.getTradeLegs().get(0).getTradeLegID()+" was not filtered");
								}
							}
						}else if(numberOfLeg > 1){
							List<String> featuresId = new ArrayList<>();
							for(Feature feature: mapOfXmlObjects.get(entry.getKey()).getValuation().getFeature()){
								featuresId.add(feature.getCashflow().getLegId());
							}
							for(String id : featuresId){
								if(trade.getTradeLegs().get(0).getTradeLegID().equals(id)){
									results.put("Feature with id "+ id + " was created", "PASS");
								}
							}
						}
					 }else if(!flag){ //discarded
						 
						 
						 measures = trade.getTradeLegs().get(0).getMeasures();
							for (DataSourcingPack.PnL_Credit.JsonObjectClasses.Measures measure : measures){
								if (!measure.getMeasureStandardName().equals(outOfScope) && measure.getCurrencyType().equals("TXC")){
									results.put("Check generated TADS-file, ensure that " + trade.getTradeLegs().get(0).getTradeLegID()+" was filtered", "PASS");
									Common.print("Check generated TADS-file, ensure that " + trade.getTradeLegs().get(0).getTradeLegID()+" was filtered");
								}
							}
					 }
				 }
			}
		}
		return results;
	}
	private StringBuilder getProcessStatus(
			Map<String, TaMessage> mapOfXmlObjects, Entry entry) {
		StringBuilder status = new StringBuilder();
		status.append("status : ");
		status.append(mapOfXmlObjects.get(entry.getKey()).getProcessStatus().getStatus());
		status.append("|");
		if(mapOfXmlObjects.get(entry.getKey()).getProcessStatus().getExceptionList()!=null){
			if(mapOfXmlObjects.get(entry.getKey()).getProcessStatus().getExceptionList().getException().size()!=0){
			status.append("error description : ");
			status.append(mapOfXmlObjects.get(entry.getKey()).getProcessStatus().getExceptionList().getException().get(0).getDescription());
			status.append("|");
			status.append("error level : ");
			status.append(mapOfXmlObjects.get(entry.getKey()).getProcessStatus().getExceptionList().getException().get(0).getLevel());
			status.append("|");
			status.append("error type : ");
			status.append(mapOfXmlObjects.get(entry.getKey()).getProcessStatus().getExceptionList().getException().get(0).getType());
			}
			
		} else {
			status.append("filter id: ");
			status.append(mapOfXmlObjects.get(entry.getKey()).getProcessStatus().getFilter().getFilterId());
			status.append("|");
			status.append("filter description : ");
			status.append(mapOfXmlObjects.get(entry.getKey()).getProcessStatus().getFilter().getFilterDescription());
		}
		return status;
	}
	
	 protected Map<String, String> verifyFilteringLegalEntity(PnLReport jsonForTest, Map<String, TaMessage> mapOfXmlObjects, Map<String, String> results, String flag){
		 String[] outOfScopeList = {"0861","0839","0550","6201","6401","5006"};
		 List<String> status = new ArrayList<>();
		 String attributValue = null;
		 LegalEntity legalEntity;
		 for(Entry entry: mapOfXmlObjects.entrySet()){
			 for(Trades trade : jsonForTest.getTraderBook().get(0).getTrades()){
				 if(entry.getKey().equals(trade.getTradeId())){ // get correspondent Trade object
					 for(String entity : outOfScopeList ){
					 if(flag.equals("in")){
						 attributValue = mapOfXmlObjects.get(trade.getTradeId()).getValuation().
									getBook().getLegalEntity().getId(); //get legal_entity id from TADS 
						 if(entity.equals(attributValue) || attributValue == null){
								status.add("Tads with tradeId: " + attributValue + " was not filtered"+"Value: PASS"); // "LegalEntity " + attributeValue + " was not filtered"+"Value: PASS"
								break;
									}
					 }else if(flag.equals("not in")){
						 legalEntity = mapOfXmlObjects.get(trade.getTradeId()).getValuation().
									getBook().getLegalEntity();
						 if(legalEntity == null){
							 builder = getProcessStatus(mapOfXmlObjects, entry); //get exception or filter description
							 results.put(" TADS with tradeId"  + entry.getKey()+ "was filtered " +"====>"+builder.toString(), "PASS");
							 break;
						 }
					 }
				 }
					//results.put("Tads with tradeId: "+entry.getKey(), "PASS");
					for(String s: status){
						if(s.contains("PASS")){
							results.put( s, "PASS");
						}else results.put("TradeId"+entry.getKey(), "FAIL");
					}
					
					status.clear();
				 }
			 }
		 }
		return results;
		 
	 }
	 @Deprecated
	 protected Map<String, String> verifyFiltering(PnLReport jsonForTest, Map<String, TaMessage> mapOfXmlObjects,
				List<List<String>> outOfSkopeList, Map<String, String> results, String flag) {
		 List<String> status = new ArrayList<>();
	    	String attributValue = null;
		 for(Entry entry: mapOfXmlObjects.entrySet()){
				for(Trades trade : jsonForTest.getTraderBook().get(0).getTrades()){
					if(entry.getKey().equals(trade.getTradeId())){ // get correspondent Trade object
						for(List<String> lists : outOfSkopeList ){
							switch(lists.get(0).trim()){
							case "FinancialProductTypeId": attributValue = trade.getFinancialProductTypeId();break;
							case "TradeSourceSystemName" : attributValue = trade.getTradeSourceSystemName();break;
							case "SettlementSystemName" : attributValue = trade.getSettlementSystemName();break;
							case "legal_entity" : attributValue = mapOfXmlObjects.get(trade.getTradeId()).getValuation().
									getBook().getLegalEntity().getId();
							}
							if(flag.equals("in")){
								if(lists.get(1).trim().equals(attributValue) || attributValue == null){
									status.add(lists.get(0)+" was filtered "+" Value: PASS "+attributValue);
										}
									} else if (flag.equals("not in")){                                                      //!
										if(lists.get(1).trim()!=attributValue){
											status.add(lists.get(0)+" was filtered "+" Value: PASS "+attributValue);
										}
									}
									}
										results.put("Tads with tradeId: "+entry.getKey(), "PASS");
										builder = getProcessStatus(mapOfXmlObjects, entry);
										results.put(entry.getKey()+" "+builder.toString(), "PASS");
											for(int i = 0; i < status.size();i++){
												System.out.println(status.get(i));
												results.put( "TradeId: " + entry.getKey() +" "+ status.get(i), "PASS");
												}
									for(String s: status){
										if(s.contains("PASS")){
											results.put("TradeId"+entry.getKey(), "PASS");
										}else results.put("TradeId"+entry.getKey(), "FAIL");
									}
									
									status.clear();
							}
					//
						}
					}
	    	return results;
	    }
	 
	protected Map<String, String> verifyFilteringSourseSystem(PnLReport jsonForTest, Map<String, TaMessage> mapOfXmlObjects,
			String[] outOfSkope, Map<String, String> results, boolean flag) {
		
		for(Entry entry: mapOfXmlObjects.entrySet()){
			for(Trades trade : jsonForTest.getTraderBook().get(0).getTrades()){
				if(entry.getKey().equals(trade.getTradeId()) ){
					if(trade.getSettlementSystemName()!=null && trade.getSettlementSystemName().equals("SUMMIT_GBO")){
						Common.print(entry.getKey()+" SettlementSystemName=SUMMIT_GBO");
						results.put(entry.getKey()+" SettlementSystemName=SUMMIT_GBO", "PASS");
					}
					
					for(String finProdTypeId  : outOfSkope ){
						if(flag){ //in scope
							if(finProdTypeId.equals(trade.getFinancialProductTypeId())){
								if(trade.getSettlementSystemName() == null && trade
									    .getTradeSourceSystemName().equals("SUMMIT") || trade
										.getTradeSourceSystemName().equals("FO3DG")){
											results.put("Ensure that PV " +entry.getKey()+"  was filtered. SettlementSystemName == null. TradeSourceSystemName = "+trade.getTradeSourceSystemName(), "PASS");
											Common.print(entry.getKey()+"in scope ");
											break;
								}
							}
						}else if(!flag){ //out osf scope
							if(finProdTypeId!=(trade.getFinancialProductTypeId())){
								if(trade.getSettlementSystemName() == null && trade
									.getTradeSourceSystemName().equals("SUMMIT") || trade
									.getTradeSourceSystemName().equals("FO3DG")){
										results.put("Ensure that PV " +entry.getKey()+"  was not filtered. SettlementSystemName == null. TradeSourceSystemName = "+trade.getTradeSourceSystemName(), "PASS");
										Common.print(entry.getKey()+"out of scope ");
										break;
								} 
							}	 
								
						 }
						}
				}
				}
		
		}
				return results;
	}

	protected Map<String, String> verifyFilteringSourseSystem(PnLReport jsonForTest, Map<String, TaMessage> mapOfXmlObjects,
			Map<String, String> results){
		for(Entry entry: mapOfXmlObjects.entrySet()){
			for(Trades trade : jsonForTest.getTraderBook().get(0).getTrades()){
				if(entry.getKey().equals(trade.getTradeId()) ){
					if(trade.getSettlementSystemName()!=null && trade.getSettlementSystemName().equals("SUMMIT_GBO")){
						Common.print(entry.getKey()+" SettlementSystemName=SUMMIT_GBO");
						results.put(entry.getKey()+" SettlementSystemName=SUMMIT_GBO", "PASS");
					} else { 
						Common.print(entry.getKey()+" SettlementSystemName=not SUMMIT_GBO");
						results.put(entry.getKey()+" SettlementSystemName!=SUMMIT_GBO", "PASS");
					}
					}
				}
						
		}
		return results;
	}
	
	protected List<List<String>> getXLSdata(String pathToFile) {
		List<List<String>> lists = null;
		try {
			lists = UtilsXLS.readXSLX(pathToFile);
		} catch (IOException e) {
			Common.print("Can't read xls file");
			e.printStackTrace();
		}
		return lists;
	}

	protected Map<String, String> getMapXLSdata(String pathToFile) {
		Map<String, String> map = new HashMap<String, String>();
		for(List<String> list : getXLSdata(pathToFile)){
			map.put(list.get(0), list.get(1));
		}
		return map;
	}
	protected Map<String, String> verifyFilteringProducts(PnLReport jsonForTest, Map<String, TaMessage> mapOfXmlObjects, Map<String, String> results) {
		 for(Entry entry: mapOfXmlObjects.entrySet()){
			 for(Trades trade : jsonForTest.getTraderBook().get(0).getTrades()){
				 if(entry.getKey().equals(trade.getTradeId())){ // get correspondent Trade object
					 results.put("PV with FinProdTypeId " + trade.getFinancialProductTypeId()+" was filtered", "PASS");
				 }	
			 }
		 }
			return results; 
		}

	protected void ensureThatFileWasNotFiltered(Map<String, String> results, int countOfUnfilteredFiles) {
		if(listOfXmlObjects.size()>0){
			if(countOfUnfilteredFiles == listOfXmlObjects.size()){
				Common.print("Count of unfiltered files = " +  mapOfXmlObjects.size());
				Common.print("List of objects size = " + listOfXmlObjects.size());
				results.put("Check generated TADS-file, ensure that files were not filtered", "PASS");
			} else results.put("Check generated TADS-file, ensure that files were not filtered", "PASS");
		} else results.put("List of objects is empty", "FAIL");
	}
	
	protected void ensureThatFileWasFiltered(Map<String, String> results, int countOfFilteredFiles) {
		if(listOfXmlObjects.size()>0){
			if(countOfFilteredFiles == listOfXmlObjects.size()){
				Common.print("Count of filtered files = " +  mapOfXmlObjects.size());
				Common.print("List of objects size = " + listOfXmlObjects.size());
				results.put("Check generated TADS-file, ensure that files were filtered", "PASS");
			} else results.put("Check generated TADS-file, ensure that files were filtered", "PASS");
		} else results.put("List of objects is empty", "FAIL");
	}
	

	protected void cleanLocalTempDirectory(String localPath) {
		try {
			FileUtils.cleanDirectory(new File(localPath));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	protected String verifyEnrichmentFinancial(PnLReport jsonForTest, Map<String, TaMessage> mapOfXmlObjects,
			Map<String, String> productFinancialMapping, Map<String, String> results) {
		int countOfEnrichedTads = 0;
		for(Entry entry: mapOfXmlObjects.entrySet()){
			 for(Trades trade : jsonForTest.getTraderBook().get(0).getTrades()){
				 if(entry.getKey().equals(trade.getTradeId())){// get correspondent Trade object
					 String financialProductTypeId = trade.getFinancialProductTypeId();
						for(Entry pairOfValues : productFinancialMapping.entrySet()){
							if(financialProductTypeId.equals(pairOfValues.getKey().toString().trim())){ // matching json and map
								if(pairOfValues.getValue().toString().trim().equals(mapOfXmlObjects // matching map and xml
										.get(entry.getKey()).getValuation().getProduct().getProductId())){
									countOfEnrichedTads++;
									StringBuilder res = new StringBuilder();
									res.append("FO Product Type for ");
									res.append(trade.getTradeId());
									res.append(" was enriched with values: ");
									res.append("Json ");
									res.append(financialProductTypeId);
									res.append("| Financial product type mapping values: ");
									res.append(pairOfValues.getKey());
									res.append(" :: ");
									res.append(pairOfValues.getValue());
									res.append("| xml ProductID - ");
									res.append(mapOfXmlObjects.get(entry.getKey()).getValuation().getProduct().getProductId());
									
									results.put(res.toString(), "PASS");
								}
							}
						}
					  
				 }
			 }	 
		}		 
		
				return String.valueOf(countOfEnrichedTads);
			}
	protected String verifyEnrichmentPRDS(PnLReport jsonForTest, Map<String, TaMessage> mapOfXmlObjects,Map<String, String> mapOfProducts, Map<String, String> results) {
		int countOfEnrichedTads = 0;
		for(Entry entry: mapOfXmlObjects.entrySet()){
			 for(Trades trade : jsonForTest.getTraderBook().get(0).getTrades()){
				 if(entry.getKey().equals(trade.getTradeId())){// get correspondent Trade object
					String jsonPRDSproductType = trade.getPRDSProductType();
					for(Entry pairOfValues : mapOfProducts.entrySet()){
						if(jsonPRDSproductType.equals(pairOfValues.getKey().toString().trim())){ // matching json and map
							if(pairOfValues.getValue().toString().trim().equals(mapOfXmlObjects // matching map and xml
									.get(entry.getKey()).getValuation().getProduct().getProductId())){
								countOfEnrichedTads++;
								StringBuilder res = new StringBuilder();
								res.append("FO Product Type for ");
								res.append(trade.getTradeId());
								res.append(" was enriched with values: ");
								res.append("Json ");
								res.append(jsonPRDSproductType);
								res.append("| PRDS product type mapping values: ");
								res.append(pairOfValues.getKey());
								res.append(" :: ");
								res.append(pairOfValues.getValue());
								res.append("| xml ProductID - ");
								res.append(mapOfXmlObjects.get(entry.getKey()).getValuation().getProduct().getProductId());
								
								results.put(res.toString(), "PASS");
							}
						}
					}
					
				 }
			 }	 
		}
		return String.valueOf(countOfEnrichedTads);
	}
	
    protected void verifyEnrichmentCombinated(PnLReport jsonForTest, Map<String, TaMessage> mapOfXmlObjects,Map<String, String> mapOfProducts, Map<String, String> productFinancialMapping, Map<String, String> results){
    	
    	for(Entry entry: mapOfXmlObjects.entrySet()){
			 for(Trades trade : jsonForTest.getTraderBook().get(0).getTrades()){
				 if(entry.getKey().equals(trade.getTradeId())){// get correspondent Trade object
					 if(trade.getPRDSProductType()!=null){
						 //get PRDS, matching  json and map, matching map and xml 
						 String jsonPRDSproductType = trade.getPRDSProductType(); 
						 for(Entry pairOfValuesPRDS : mapOfProducts.entrySet()){
								if(jsonPRDSproductType.equals(pairOfValuesPRDS.getKey().toString().trim())){ // matching json and map
									if(pairOfValuesPRDS.getValue().toString().trim().equals(mapOfXmlObjects // matching map and xml
											.get(entry.getKey()).getValuation().getProduct().getProductId())){
										StringBuilder res = new StringBuilder();
										res.append("FO Product Type for ");
										res.append(trade.getTradeId());
										res.append(" was enriched with values: ");
										res.append("Json ");
										res.append(jsonPRDSproductType);
										res.append("| PRDS product type mapping values: ");
										res.append(pairOfValuesPRDS.getKey());
										res.append(" :: ");
										res.append(pairOfValuesPRDS.getValue());
										res.append("| xml ProductID - ");
										res.append(mapOfXmlObjects.get(entry.getKey()).getValuation().getProduct().getProductId());
										
										results.put(res.toString(), "PASS");
									}
								}
						 }
					 
					 } else if(trade.getPRDSProductType()==null){
						//get Financial product type, matching  json and map, matching map and xml 
						 String financialProductTypeId = trade.getFinancialProductTypeId(); 
						 for(Entry pairOfValuesFin : productFinancialMapping.entrySet()){
								if(financialProductTypeId.equals(pairOfValuesFin.getKey().toString().trim())){ // matching json and map
									if(pairOfValuesFin.getValue().toString().trim().equals(mapOfXmlObjects // matching map and xml
											.get(entry.getKey()).getValuation().getProduct().getProductId())){
										StringBuilder res = new StringBuilder();
										res.append("FO Product Type for ");
										res.append(trade.getTradeId());
										res.append(" was enriched with values: ");
										res.append("Json ");
										res.append(financialProductTypeId);
										res.append("| Financial product type mapping values: ");
										res.append(pairOfValuesFin.getKey());
										res.append(" :: ");
										res.append(pairOfValuesFin.getValue());
										res.append("| xml ProductID - ");
										res.append(mapOfXmlObjects.get(entry.getKey()).getValuation().getProduct().getProductId());
										
										results.put(res.toString(), "PASS");
									}	
								}
						 }
					 }
				 }
			 }
    	}
    	
    }
    
    protected String verifyBookEnrichment(PnLReport jsonForTest, List<TaMessage> listOfXmlObjects, Map<String, String> results) {
    	Book book = listOfXmlObjects.get(0).getValuation().getBook();
    	BOOK staticDataRDBook  = null;
    	String bookFromRDbookString = getStaticRecordFromServiceRequest(
				jsonForTest.getTraderBook().get(0).getTraderBookCode(),
				jsonForTest.getTraderBook().get(0)
						.getTraderBookSourceSystemName());
    	if (bookFromRDbookString.contains("Exception")){
			results.put("Error when get RD book from service","FAIL");
    	} else {
    		try{
    			File bookFromRDBookXML = createXmlFileFromString(bookFromRDbookString,
    					"bookFromRDBook.xml");
    			 staticDataRDBook = createObjectFromStaticData(bookFromRDBookXML);
    			
    			 //step 3
    			 if(staticDataRDBook.getSOURCE().equals("BRDS")){
    				 if(staticDataRDBook.getREGRPTTREAT().equals("B")){
    					verifyXMLValueAgainstData("Verify is_bank_book",String.valueOf(book.isIsBankBook()) , "true", results); 
    				 } else {verifyXMLValueAgainstData("Verify is_bank_book",String.valueOf(book.isIsBankBook()) , "false", results);}
    			 } else {
    				 verifyXMLValueAgainstData("Verify is_bank_book",String.valueOf(book.isIsBankBook()) , "false", results);
    			 }
    			 //step 4
    			 verifyXMLValueAgainstData("Verify legal_entity id",book.getLegalEntity().getId() , staticDataRDBook.getLECODE(), results);
    			 verifyXMLValueAgainstData("Verify legal_entity source_system", book.getLegalEntity().getSourceSystem(), "AMI_NET", results);
    			 verifyXMLValueAgainstData("Verify legal_entity type", book.getLegalEntity().getType(), "AMI_CODE", results);
    			 // step 5
    			 verifyXMLValueAgainstData("Verify ubr_area", book.getUbrArea().get(0).getValue(), String.valueOf(staticDataRDBook.getBUSOWNERCODE()), results);
    			 //step 6
    			 verifyXMLValueAgainstData("Verify profit_center", book.getProfitCenter(), staticDataRDBook.getPROFITCENTERCODE(), results);
    			 // step 7
    			 verifyXMLValueAgainstData("Verify capacity", book.getCapacity(), "PRINCIPAL", results);
    			 // step 8
    			 verifyXMLValueAgainstData("Verify gaap",  book.getGaap(), "IFRS", results);
    			 // step 9
    			 if(staticDataRDBook.getSOURCE().equals("BRDS")){
    				switch(staticDataRDBook.getACCTREAT()){
    				case "A" : verifyXMLValueAgainstData("verify regulatory_reporting_treatment A", book.getRegulatoryReportingTreatment(), "AFS", results); break;
    				case "H" : verifyXMLValueAgainstData("verify regulatory_reporting_treatment H", book.getRegulatoryReportingTreatment(), "TRADING", results);break;
    				case "F" : verifyXMLValueAgainstData("verify regulatory_reporting_treatment F", book.getRegulatoryReportingTreatment(), "FVO", results);break;
    				case "L" : verifyXMLValueAgainstData("verify regulatory_reporting_treatment L", book.getRegulatoryReportingTreatment(), "LnR", results);break;
    				case "HM" : verifyXMLValueAgainstData("verify regulatory_reporting_treatment HM", book.getRegulatoryReportingTreatment(), "UNKNOWN", results);break;
    				case "N" : verifyXMLValueAgainstData("verify regulatory_reporting_treatment N", book.getRegulatoryReportingTreatment(), "UNKNOWN", results);break;
    				case "" : verifyXMLValueAgainstData("verify regulatory_reporting_treatment N", book.getRegulatoryReportingTreatment(), "UNKNOWN", results);break;
    				default : verifyXMLValueAgainstData("verify regulatory_reporting_treatment default", book.getRegulatoryReportingTreatment(), "UNKNOWN", results);
    				} 
    			 } else if(staticDataRDBook.getSOURCE().equals("dbNexus")){
    				 verifyXMLValueAgainstData("verify regulatory_reporting_treatment dbNexus ", book.getRegulatoryReportingTreatment(), "TRADING", results);
    			 }
    			 // step 10
    			 verifyXMLValueAgainstData("verify organization_unit_id", book.getOrganizationUnitId(), String.valueOf(staticDataRDBook.getBOOKID()), results);
    			 // step 11 
    			 verifyXMLValueAgainstData("verify organization_unit_source_system", book.getOrganizationUnitSourceSystem(), staticDataRDBook.getBookIdSource(), results);
    			 // step 12
    			 verifyXMLValueAgainstData("verify organization_unit_type", book.getOrganizationUnitType(), staticDataRDBook.getBOOKTYPE(), results);
    		} catch (Exception e) {
    			results.put("Error when try to parse RD book","FAIL");
    			
    		}
    		
    		
    	}
    	return null;
    }
    
    
    
	protected String verifyThatAllFilesWasEnriched(Map<String, TaMessage> mapOfXmlObjects, String countOfEnrichedTads) {
		String res  = "";
		if(mapOfXmlObjects.size() == Integer.valueOf(countOfEnrichedTads)){
			res = "PASS";
		}else {res = "FAIL";
		Common.print("Count of parsed TADS = "+ mapOfXmlObjects.size());
		Common.print("Count of Enriched TADS = " + countOfEnrichedTads);
		}
		
		return res;
	}
	

	protected BOOK getRDBook(PnLReport jsonForTest) {
		String bookFromRDbookString = getStaticRecordFromServiceRequest(
				jsonForTest.getTraderBook().get(0).getTraderBookCode(),
				jsonForTest.getTraderBook().get(0)
						.getTraderBookSourceSystemName());
		BOOK staticDataRDBook;
		
		if(bookFromRDbookString.contains("Exception")){
			staticDataRDBook = null;
		} else{
		File bookFromRDBookXML = createXmlFileFromString(bookFromRDbookString,
				"bookFromRDBook.xml");
		staticDataRDBook = createObjectFromStaticData(bookFromRDBookXML);
		}
		return staticDataRDBook;
		
	}

	protected void verifyAgainstRDbook(String step, String xmlString,
		String rdbookString, Map<String, String> results) {
		verifyXMLValueAgainstData(step, xmlString, rdbookString, results);
			}

	protected void verifyThatEntityNotExists(String step, Object entity,
			Map<String, String> results) {
				if(entity!=null){
					results.put(step, "FAIL");
				} else results.put(step, "PASS");
				
	}
	
	/**
	 * Replaces global tags in globalVariables
	 * 
	 * @param string_with_tags
	 * @return
	 */
	protected static String substitute(String string_with_tags) {
		return Common.replaceGlobalTags(string_with_tags, props);
	}

	/**
	 * Get .json files from folder used regex mask
	 * 
	 * @param folder
	 * @return list of files
	 */
	public static List<String> getJsonFileInDir(String folder) {
		// get mask for message files
		String regex_mask = ".*.json";
		// Common.print("TestFolder: " + TestFolder );
		return Common.listFilesForFolder(new File(folder), regex_mask);
	}
	
	// TODO CONTINUE
		/*
		 * private TaMessage getTaMessageFromString(String taMessageString, Report
		 * reportFile){ TaMessage message = null;
		 * 
		 * xmlParsingInObject(pathToFile, reportFile); return message; }
		 */
	/**
	 * Validate .xml against .xsd schema
	 * 
	 * @param xml
	 * @param xsd
	 * @return
	 */
	/*
	 * static String validateAgainstXSD(InputStream xml, InputStream xsd) {
	 * String res = ""; try { SchemaFactory factory =
	 * SchemaFactory.newInstance(XMLConstants
	 * .DEFAULT_NS_PREFIX);//W3C_XML_SCHEMA_NS_URI); File schemaLocation = new
	 * File(xsdPathForTest); Schema schema = factory.newSchema(schemaLocation);
	 * Validator validator = schema.newValidator(); validator.validate(new
	 * StreamSource(xml)); res = "TRUE"; } catch (Exception ex) {
	 * System.out.println(ex); res = "FALSE"; } return res; }
	 */
}