

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.bind.JAXBException;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import DataSourcingPack.PnL_Credit.JsonObjectClasses.FormJsonSource;
import DataSourcingPack.PnL_Credit.JsonObjectClasses.Measures;
import DataSourcingPack.PnL_Credit.JsonObjectClasses.PnLReport;
import DataSourcingPack.PnL_Credit.JsonObjectClasses.Trades;
import DataSourcingPack.PnL_Credit.StaticDataClasses.RDBook.BOOK;
import UtilsPack.Common;
import UtilsPack.Report;
import UtilsPack.TemplateActions;
import UtilsPack.TimeCounter;
import UtilsPack.UtilsSQL;
import UtilsPack.WorkWithProperties;

import org.testng.annotations.Parameters;

public class NYTadsMsgGenerationTest extends TestHelper {
	
	public static final String COMMONTESTFOLDER = "/04_TADS-message generation/";
	public static String localPath;
	private static ArrayList<String> listOfJsonDatasets = new ArrayList<String>();
	ArrayList<PnLReport> listWithJsonsObjects;
	Map<String, PnLReport> mapOfJsons = new HashMap<String, PnLReport>();
	private static Map <String, String> mapOfWorkflowId;
	private long totalSuiteDurationStart;
	
	String testDataFolder;
	
	@BeforeSuite
	@Parameters({ "configFile" })
	public void testPreparation(String configFile) throws FileNotFoundException {
		totalSuiteDurationStart = TimeCounter.getCurrentTime();
		System.out.println("===================> before suit started");
		// read properties file and variables initialization 
		props = WorkWithProperties.LoadProps(".\\TestData\\" + configFile); 
		basicPath = props.getProperty("PATH");
		localPath = basicPath + "\\temp";
		unixFolderPath = props.getProperty("UNIX_PATH_DATA");
		props.put(
				"SCRIPTS_FOLDER",
				"C:\\Users\\shevvla\\SVN\\Automation_Luxoft\\TA\\TestData\\DS\\PnL_Credit\\Scripts");
		ScriptsPath = props.getProperty("SCRIPTS_FOLDER");
		BUSINESS_LINE = props.getProperty("BUSINESS_LINE");
		//create report file
		Date reportDate = new Date();
		reportFile = new Report(".\\Reports\\" + BUSINESS_LINE + "_"
				+ DateFormat.format(reportDate)+"_generation" + ".xlsx");
		startTime = TimeCounter.getCurrentTime();
		
		String result = "";
//		String commandToRunForClean = "cd %%UNIX_PATH_DATA%%; rm *.json; ls; ";
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
			//uploadToUnix
			result += uploadToUnix(fileTestName, jsonFilePathToCopy);
			//uploadToHadoop
			uploadToHadoop();
			props.put("FEEDID", "18"); // 7 LONDON 18 NY
			props.put("ORGLSRCID", "CREDIT");
			props.put("FEEDENTITYID", "DBNY_CAP_LCH");
			//executeInsert
			result += executeInsert();
			//wait 8 min 
			try {
				Thread.sleep(480000);
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
		//get collection of workflow id and datasets 
		mapOfWorkflowId = getMapOfWorflowIdAndDataSets(listOfJsonDatasets);
		reportFile.addFinal("PASS",startTime);
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
				props.put("JSON_DATASET_UPDATED_NAME", dataset);
				
			} 
		}
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
	public void bookGenerationTest() throws JAXBException {
		System.out.println("bookGenerationTest test started");
		Map<String, String> results = new LinkedHashMap<String, String>();
		downloadFromUnixToLocal(props.getProperty("TADSFOLDERONUNIX"));
		// get .xml file and put it into object taMessage
		listOfXmlObjects = getTaMessages(localPath, results);
		mapOfXmlObjects = getCompareMapOfXmlObjects();
		// preparing entities to test
		Book book = listOfXmlObjects.get(0).getValuation().getBook();

		// step 1
		// results.put("Validate xml file", validateAgainstXSD(xml, xsd));
		// validateAgainstXSD(xml, xsd); //need good schema

		verifyXMLValueAgainstData("Verify book Name", book.getName(),
				jsonForTest.getTraderBook().get(0).getTraderBookCode(), results);
		verifyXMLValueAgainstData("Verify book Source system",
				book.getSourceSystem(), jsonForTest.getTraderBook().get(0)
						.getTraderBookSourceSystemName(), results);
		verifyXMLValueAgainstData("Verify book Capacity", book.getCapacity(),
				"PRINCIPAL", results);
		verifyXMLValueAgainstData("Verify book Legal entity Source System",
				book.getLegalEntity().getSourceSystem(), "AMI_NET", results);
		verifyXMLValueAgainstData("Verify book Legal entity Type", book
				.getLegalEntity().getType(), "AMI_CODE", results);
		// against RDBook
		String bookFromRDbookString = getStaticRecordFromServiceRequest(
				jsonForTest.getTraderBook().get(0).getTraderBookCode(),
				jsonForTest.getTraderBook().get(0)
						.getTraderBookSourceSystemName());
		
		if(bookFromRDbookString.contains("Exception")){
			results.put("against RD BOOK","FAIL");
		} else {
		BOOK staticDataRDBook;
	try {
		File bookFromRDBookXML = createXmlFileFromString(bookFromRDbookString,
				"bookFromRDBook.xml");
		 staticDataRDBook = createObjectFromStaticData(bookFromRDBookXML);
	
		
		verifyXMLValueAgainstData("Verify book Legal entity Id", book
				.getLegalEntity().getId(), String.valueOf(staticDataRDBook
				.getLECODE()), results);
		verifyXMLValueAgainstData("Verify book Profit centre",
				book.getProfitCenter(), staticDataRDBook.getPROFITCENTERCODE(),
				results);
		if (staticDataRDBook.getACCTREAT().equals("HM")) {
			verifyXMLValueAgainstData(
					"Verify book Regulatory Reporting Treatment",
					book.getRegulatoryReportingTreatment(), "UNKNOWN", results);
		}
		verifyXMLValueAgainstData("Verify book Organization Unit Id",
				book.getOrganizationUnitId(),
				String.valueOf(staticDataRDBook.getBOOKID()), results);
		verifyXMLValueAgainstData("Verify book Organization Unit Type",
				book.getOrganizationUnitType(), staticDataRDBook.getBOOKTYPE(),
				results);
		verifyXMLValueAgainstData("Verify book Ubr Area ", book.getUbrArea()
				.get(0).getValue(),
				String.valueOf(staticDataRDBook.getBUSOWNERCODE()), results);
		verifyXMLValueAgainstData("Verify book Gaap", book.getGaap(), "IFRS",
				results);
} catch(Exception e){
		results.put("against RD BOOK","FAIL");
	}
		}
		updateReportWithTestResults(results);
		updateReportToFinal(reportFile, results, startTime);
		System.out.println("bookGenerationTest test finished");

	}

	@Test
	public void counterparty_enrich_External_role() {
		Common.print("counterparty_enrich_External_role test started");
		Map<String, String> results = new LinkedHashMap<String, String>();
		downloadFromUnixToLocal(props.getProperty("TADSFOLDERONUNIX"));
		// preparing entities to test
		
		
		listOfXmlObjects = getTaMessages(localPath, results);
		mapOfXmlObjects = getCompareMapOfXmlObjects();
		
		TaMessage PV1 = mapOfXmlObjects.get(jsonForTest.getTraderBook().get(0)
				.getTrades().get(0).getTradeId());
		TaMessage PV2 = mapOfXmlObjects.get(jsonForTest.getTraderBook().get(0)
				.getTrades().get(1).getTradeId());
		Counterparty counterPartyExternalForPV1 = PV1.getValuation().getCounterParty();
		Counterparty counterPartyExternalForPV2 = PV2.getValuation().getCounterParty();
		Trades tradesForPV1 = jsonForTest.getTraderBook().get(0).getTrades().get(0);
		Trades tradesForPV2 = jsonForTest.getTraderBook().get(0).getTrades().get(1);
		//Check TADS-message for PV#1
		results.put("Check TADS-message for PV#1", "");
		verifyXMLValueAgainstData("Verify counterparty ID",
				counterPartyExternalForPV1.getId(), tradesForPV1.getPartyId(), results);
		verifyXMLValueAgainstData("Verify that type attribute is present", counterPartyExternalForPV1.
				getType(), "L_CPTY_ID", results);
		verifyXMLValueAgainstData("Verify that role attribute inside is present", counterPartyExternalForPV1.
				getRole(), "EXTERNAL", results);
		verifyXMLValueAgainstData("Verify counterparty source_system", counterPartyExternalForPV1.getSourceSystem(), tradesForPV1.getPartyIdSourceSystem(), results);
		verifyThatEntityNotExists( "\"legal_entity\" attribute does not exist for \"counter_party\" entity in TADS-message", 
				counterPartyExternalForPV1.getLegalEntity(), results);
		
		
		//Check TADS-message for PV#2
		results.put("Check TADS-message for PV#2", "");
		verifyXMLValueAgainstData("Verify counterparty ID for PV 2",
				counterPartyExternalForPV2.getId(), tradesForPV2.getPartyId(), results);
		verifyXMLValueAgainstData("Verify counterparty source system for PV 2",
				counterPartyExternalForPV2.getSourceSystem(),
				tradesForPV2.getPartyIdSourceSystem(), results);
		verifyXMLValueAgainstData("Verify that type attribute is present for PV 2", counterPartyExternalForPV2.
				getType(), "L_CPTY_ID", results);
		verifyXMLValueAgainstData("Verify that role attribute inside is present for PV 2", counterPartyExternalForPV2.
				getRole(), "EXTERNAL", results);
		verifyThatEntityNotExists( "\"legal_entity\" attribute does not exist for \"counter_party\" entity in TADS-message for PV#2", 
				counterPartyExternalForPV2.getLegalEntity(), results);

		updateReportWithTestResults(results);
		updateReportToFinal(reportFile, results, startTime);
		System.out.println("counterPartyGenerationTestEXTERNAL test finished");

	}

	@Test
	public void counterparty_enrich_Internal_role1_sameLE() {
		Common.print("counterparty_enrich_Internal_role1_sameLE test started");
		Map<String, String> results = new LinkedHashMap<String, String>();
		downloadFromUnixToLocal(props.getProperty("TADSFOLDERONUNIX"));
		listOfXmlObjects = getTaMessages(localPath, results);
		mapOfXmlObjects = getCompareMapOfXmlObjects();
		// preparing entities to test
		// PV#3
		Counterparty counterPartyInternal = listOfXmlObjects.get(0)
				.getValuation().getCounterParty();
		Trades trades = jsonForTest.getTraderBook().get(0).getTrades().get(0);

		verifyXMLValueAgainstData("Verify counterparty ID",
				counterPartyInternal.getId(), trades.getPartyId(), results);
		verifyXMLValueAgainstData("Verify counterparty source system",
				counterPartyInternal.getSourceSystem(),
				trades.getPartyIdSourceSystem(), results);
		verifyXMLValueAgainstData("Verify counterparty role ",
				counterPartyInternal.getRole(), "INTRA", results);
		verifyXMLValueAgainstData("Verify counterparty type",
				counterPartyInternal.getType(), "L_CPTY_ID", results);
		if(counterPartyInternal.getLegalEntity()!=null){
		if (getRDBook(jsonForTest)!=null){
		verifyAgainstRDbook("The \"id\" attribute inside  \"legal_entity\" attribute is populated from <LE_CODE> from RD_BOOK",
				counterPartyInternal.getLegalEntity().getId(), getRDBook(jsonForTest).getLECODE(), results );
		} else {
			results.put("RDBook creation", "FAIL");
		}} else {results.put("legal entyty is null", "FAIL");}
		
		updateReportWithTestResults(results);
		updateReportToFinal(reportFile, results, startTime);
		System.out.println("counterparty_enrich_Internal_role1_sameLE test finished");
	}

	@Test
	public void counterparty_enrich_Internal_role2_anotherLE() {
		Common.print("counterparty_enrich_Internal_role2_anotherLE test started");
		Map<String, String> results = new LinkedHashMap<String, String>();
		downloadFromUnixToLocal(props.getProperty("TADSFOLDERONUNIX"));
		listOfXmlObjects = getTaMessages(localPath, results);
		mapOfXmlObjects = getCompareMapOfXmlObjects();
		// preparing entities to test
		// PV#4
		Counterparty counterParty_role2 = listOfXmlObjects.get(0).getValuation()
				.getCounterParty();
		Trades trades = jsonForTest.getTraderBook().get(0).getTrades().get(0);

		verifyXMLValueAgainstData("Verify counterparty ID",
				counterParty_role2.getId(), trades.getPartyId(), results);
		verifyXMLValueAgainstData("Verify counterparty source system",
				counterParty_role2.getSourceSystem(),
				trades.getPartyIdSourceSystem(), results);
		verifyXMLValueAgainstData("Verify counterparty type",
				counterParty_role2.getType(), "L_CPTY_ID", results);
		verifyXMLValueAgainstData("Verify counterparty role ",
				counterParty_role2.getRole(), "INTER", results);
		
		if(counterParty_role2.getLegalEntity()!=null){
			if (getRDBook(jsonForTest)!=null){
			verifyAgainstRDbook("The \"id\" attribute inside  \"legal_entity\" attribute is populated from <LE_CODE> from RD_BOOK",
					counterParty_role2.getLegalEntity().getId(), getRDBook(jsonForTest).getLECODE(), results );
			} else {
				results.put("RDBook creation", "FAIL");
			}} else {results.put("legal entyty is null", "FAIL");}

		updateReportWithTestResults(results);
		updateReportToFinal(reportFile, results, startTime);
		System.out.println("counterparty_enrich_Internal_role2_anotherLE test finished");
	}

	@Test
	public void counterparty_enrich_Internal_role3_withoutLE(){
		Common.print("counterparty_enrich_Internal_role3_withoutLE test started");
		Map<String, String> results = new LinkedHashMap<String, String>();
		downloadFromUnixToLocal(props.getProperty("TADSFOLDERONUNIX"));
		listOfXmlObjects = getTaMessages(localPath, results);
		mapOfXmlObjects = getCompareMapOfXmlObjects();
		// preparing entities to test
		// PV#5
		Counterparty counterParty_role3 = listOfXmlObjects.get(0).getValuation()
				.getCounterParty();
		Trades trades = jsonForTest.getTraderBook().get(0).getTrades().get(0);
		
		verifyXMLValueAgainstData("Verify counterparty ID",
				counterParty_role3.getId(), trades.getPartyId(), results);
		verifyXMLValueAgainstData("Verify counterparty source system",
				counterParty_role3.getSourceSystem(),
				trades.getPartyIdSourceSystem(), results);
		verifyXMLValueAgainstData("Verify counterparty type",
				counterParty_role3.getType(), "L_CPTY_ID", results);
		verifyXMLValueAgainstData("Verify counterparty role ",
				counterParty_role3.getRole(), "INTER", results);
		verifyThatEntityNotExists( "\"legal_entity\" attribute does not exist for \"counter_party\" entity in TADS-message", 
				counterParty_role3.getLegalEntity(), results);
		
		
		updateReportWithTestResults(results);
		updateReportToFinal(reportFile, results, startTime);
		System.out.println("counterparty_enrich_Internal_role3_withoutLE test finished");
	}
	
	@Test
	public void cashFlowGenerationTest() {
		downloadFromUnixToLocal(props.getProperty("TADSFOLDERONUNIX"));
		Map<String, String> results = new LinkedHashMap<String, String>();
		System.out.println("cashFlowGenerationTest started");
		listOfXmlObjects = getTaMessages(localPath, results);
		// startTime = TimeCounter.getCurrentTime();

		CashFlow cashFlow = listOfXmlObjects.get(0).getValuation().getFeature()
				.get(0).getCashflow();
		Measures currencyTypeTXC = jsonForTest.getTraderBook().get(0)
				.getTrades().get(0).getTradeLegs().get(0).getMeasures().get(0);
		Measures currencyTypeLFC = jsonForTest.getTraderBook().get(0)
				.getTrades().get(0).getTradeLegs().get(0).getMeasures().get(1);

		verifyXMLValueAgainstData("Verify \"cash_flow_name\" attribute is populated correctly.",
				cashFlow.getCashFlowName(), "UNREALISEDPL", results);
		verifyXMLValueAgainstData("Verify \"amount\" attribute is populated correctly.", 
				cashFlow.getAmount().toString(), currencyTypeTXC.getAmount(), results);
		verifyXMLValueAgainstData("Verify \"currency\" attribute is populated correctly.",
				cashFlow.getCurrency(), currencyTypeTXC.getCurrencyCode(), results);
		verifyXMLValueAgainstData("Verify \"lfc_amount\" attribute is populated correctly.",
				cashFlow.getLfcAmount().toString(),
				currencyTypeLFC.getAmount(), results);
		verifyXMLValueAgainstData("Verify \"lfc_currency\" attribute is populated correctly.",
				cashFlow.getLfcCurrency(), 
				currencyTypeLFC.getCurrencyCode(),results);
		verifyXMLValueAgainstData("Verify \"value_date\" attribute is populated correctly",
				cashFlow.getValueDate().toString(), 
				jsonForTest.getCOBDate().substring(0, 10), results);
		verifyXMLValueAgainstData("Verify \"leg_id\" attribute is populated correctly",
				cashFlow.getLegId(),
				jsonForTest.getTraderBook().get(0).getTrades().get(0).getTradeLegs().get(0).getTradeLegID(), results);

		updateReportWithTestResults(results);
		updateReportToFinal(reportFile, results, startTime);
		System.out.println("cashFlowGenerationTest ended");
	}

	@Test
	public void featureGenerationTestB() {
		Common.print("featureGenerationTestB test started");
		Map<String, String> results = new LinkedHashMap<String, String>();
		downloadFromUnixToLocal(props.getProperty("TADSFOLDERONUNIX"));
		listOfXmlObjects = getTaMessages(localPath, results);
		Feature feature = listOfXmlObjects.get(0).getValuation().getFeature()
				.get(0);

		verifyXMLValueAgainstData("Verify feature type", feature.getType(),
				"PV", results);
		verifyXMLValueAgainstData("Verify feature direction",
				feature.getDirection(), "P", results);

		updateReportWithTestResults(results);
		updateReportToFinal(reportFile, results, startTime);
		System.out.println("featureGenerationTestB test finished");

	}

	@Test
	public void featureGenerationTestS() { //
		Common.print("featureGenerationTestS test started");
		Map<String, String> results = new LinkedHashMap<String, String>();
		downloadFromUnixToLocal(props.getProperty("TADSFOLDERONUNIX"));
		listOfXmlObjects = getTaMessages(localPath, results);
		Feature feature = listOfXmlObjects.get(0).getValuation().getFeature()
				.get(0);

		verifyXMLValueAgainstData("Verify feature type", feature.getType(),
				"PV", results);
		verifyXMLValueAgainstData("Verify feature direction",
				feature.getDirection(), "R", results);

		updateReportWithTestResults(results);
		updateReportToFinal(reportFile, results, startTime);
		System.out.println("featureGenerationTestS test finished");

	}

	@Test //dataset 1
	public void featureGenerationMultiLeg_1LFC2TXC() {
		Common.print("featureGenerationMultiLeg_1LFC2TXC test started");
		Map<String, String> results = new LinkedHashMap<String, String>();
		downloadFromUnixToLocal(props.getProperty("TADSFOLDERONUNIX"));
		listOfXmlObjects = getTaMessages(localPath, results);
		
		List <Feature> listOfFeatures = new ArrayList<Feature>();
		listOfFeatures = listOfXmlObjects.get(0).getValuation().getFeature();
		Feature feature1 = listOfFeatures.get(0);
		Feature feature2 = listOfFeatures.get(1);
		Feature feature3 = listOfFeatures.get(2);
		
		verifyXMLValueAgainstData("currency feature 1 EUR", feature1.getCashflow().getCurrency(), "EUR", results);
		verifyXMLValueAgainstData("currency feature 2 EUR", feature2.getCashflow().getCurrency(), "EUR", results);
		verifyXMLValueAgainstData("currency feature 3 USD", feature3.getCashflow().getCurrency(), "USD", results);
		
		updateReportWithTestResults(results);
		updateReportToFinal(reportFile, results, startTime);
		System.out.println("featureGenerationMultiLeg_1LFC2TXC test finished");
	}
	
	@Test //dataset 2
	public void featureGenerationMultiLeg_2TXC1LFC() {
		Common.print("featureGenerationTestS test started");
		Map<String, String> results = new LinkedHashMap<String, String>();
		downloadFromUnixToLocal(props.getProperty("TADSFOLDERONUNIX"));
		listOfXmlObjects = getTaMessages(localPath, results);
		
		List <Feature> listOfFeatures = new ArrayList<Feature>();
		listOfFeatures = listOfXmlObjects.get(0).getValuation().getFeature();
		Feature feature1 = listOfFeatures.get(0);
		Feature feature2 = listOfFeatures.get(1);
		Feature feature3 = listOfFeatures.get(2);
		
		verifyXMLValueAgainstData("currency feature 1 EUR", feature1.getCashflow().getCurrency(), "EUR", results);
		verifyXMLValueAgainstData("currency feature 2 USD", feature2.getCashflow().getCurrency(), "USD", results);
		verifyXMLValueAgainstData("currency feature 3 EUR", feature3.getCashflow().getCurrency(), "EUR", results);
		
		updateReportWithTestResults(results);
		updateReportToFinal(reportFile, results, startTime);
		System.out.println("featureGenerationTestS test finished");
	}
	
	@Test //dataset 3
	public void featureGenerationMultiLeg_2Leg1LFC2TXC() {
		Common.print("featureGenerationTestS test started");
		Map<String, String> results = new LinkedHashMap<String, String>();
		downloadFromUnixToLocal(props.getProperty("TADSFOLDERONUNIX"));
		listOfXmlObjects = getTaMessages(localPath, results);
		
		List <Feature> listOfFeatures = new ArrayList<Feature>();
		listOfFeatures = listOfXmlObjects.get(0).getValuation().getFeature();
		
		Feature leg1Feature1 = listOfFeatures.get(0);
		Feature leg1Feature2 = listOfFeatures.get(1);
		Feature leg1Feature3 = listOfFeatures.get(2);
		Feature leg2Feature1 = listOfFeatures.get(3);
		Feature leg2Feature2 = listOfFeatures.get(4);
		Feature leg2Feature3 = listOfFeatures.get(5);
		
		verifyXMLValueAgainstData("currency leg1Feature1 EUR", leg1Feature1.getCashflow().getCurrency(), "EUR", results);
		verifyXMLValueAgainstData("currency leg1Feature2 CAD", leg1Feature2.getCashflow().getCurrency(), "CAD", results);
		verifyXMLValueAgainstData("currency leg1Feature3 UAH", leg1Feature3.getCashflow().getCurrency(), "UAH", results);
		verifyXMLValueAgainstData("currency leg2Feature1 EUR", leg2Feature1.getCashflow().getCurrency(), "EUR", results);
		verifyXMLValueAgainstData("currency leg2Feature2 EUR", leg2Feature2.getCashflow().getCurrency(), "EUR", results);
		verifyXMLValueAgainstData("currency leg2Feature3 USD", leg2Feature3.getCashflow().getCurrency(), "USD", results);
		
		updateReportWithTestResults(results);
		updateReportToFinal(reportFile, results, startTime);
		System.out.println("featureGenerationTestS test finished");
	}
	
	@Test //dataset 4
	public void featureGenerationMultiLeg_2Leg2TXC1LFC() {
		Common.print("featureGenerationTestS test started");
		Map<String, String> results = new LinkedHashMap<String, String>();
		downloadFromUnixToLocal(props.getProperty("TADSFOLDERONUNIX"));
		listOfXmlObjects = getTaMessages(localPath, results);
		List <Feature> listOfFeatures = new ArrayList<Feature>();
		listOfFeatures = listOfXmlObjects.get(0).getValuation().getFeature();
		
		Feature leg1Feature1 = listOfFeatures.get(0);
		Feature leg1Feature2 = listOfFeatures.get(1);
		Feature leg1Feature3 = listOfFeatures.get(2);
		Feature leg2Feature1 = listOfFeatures.get(3);
		Feature leg2Feature2 = listOfFeatures.get(4);
		Feature leg2Feature3 = listOfFeatures.get(5);
		
		verifyXMLValueAgainstData("currency leg1Feature1 CAD", leg1Feature1.getCashflow().getCurrency(), "CAD", results);
		verifyXMLValueAgainstData("currency leg1Feature2 UAH", leg1Feature2.getCashflow().getCurrency(), "UAH", results);
		verifyXMLValueAgainstData("currency leg1Feature3 EUR", leg1Feature3.getCashflow().getCurrency(), "EUR", results);
		verifyXMLValueAgainstData("currency leg2Feature1 EUR", leg2Feature1.getCashflow().getCurrency(), "EUR", results);
		verifyXMLValueAgainstData("currency leg2Feature2 USD", leg2Feature2.getCashflow().getCurrency(), "USD", results);
		verifyXMLValueAgainstData("currency leg2Feature3 EUR", leg2Feature3.getCashflow().getCurrency(), "EUR", results);
		
		updateReportWithTestResults(results);
		updateReportToFinal(reportFile, results, startTime);
		System.out.println("featureGenerationTestS test finished");
	}
	
	@Test //dataset 5
	public void featureGenerationMultiLeg_2Leg1LFC1TXC() {
		Common.print("featureGenerationTestS test started");
		Map<String, String> results = new LinkedHashMap<String, String>();
		downloadFromUnixToLocal(props.getProperty("TADSFOLDERONUNIX"));
		listOfXmlObjects = getTaMessages(localPath, results);
		List <Feature> listOfFeatures = new ArrayList<Feature>();
		listOfFeatures = listOfXmlObjects.get(0).getValuation().getFeature();
		Feature leg1Feature1 = listOfFeatures.get(0);
		Feature leg1Feature2 = listOfFeatures.get(1);
		Feature leg1Feature3 = listOfFeatures.get(2);
		Feature leg2Feature1 = listOfFeatures.get(3);
		Feature leg2Feature2 = listOfFeatures.get(4);
		Feature leg2Feature3 = listOfFeatures.get(5);
		
		verifyXMLValueAgainstData("currency leg1Feature2 CAD", leg1Feature2.getCashflow().getCurrency(), "CAD", results);
		//verifyXMLValueAgainstData("currency leg2Feature1 EUR", leg2Feature1.getCashflow().getCurrency(), "EUR", results);
		verifyXMLValueAgainstData("currency leg2Feature2 EUR", leg2Feature2.getCashflow().getCurrency(), "EUR", results);
		
		updateReportWithTestResults(results);
		updateReportToFinal(reportFile, results, startTime);
		System.out.println("featureGenerationTestS test finished");
	}
	
	@Test //dataset 6
	public void featureGenerationMultiLeg_Leg2LFC2TXC_Leg1LFC1TXC() {
		Common.print("featureGenerationTestS test started");
		Map<String, String> results = new LinkedHashMap<String, String>();
		downloadFromUnixToLocal(props.getProperty("TADSFOLDERONUNIX"));
		listOfXmlObjects = getTaMessages(localPath, results);
		mapOfXmlObjects = getCompareMapOfXmlObjects();
		
		TaMessage PV1 = mapOfXmlObjects.get(jsonForTest.getTraderBook().get(0)
				.getTrades().get(0).getTradeId());
		TaMessage PV2 = mapOfXmlObjects.get(jsonForTest.getTraderBook().get(0)
				.getTrades().get(1).getTradeId());
		List <Feature> listOfFeaturesPV1 = new ArrayList<Feature>();
		List <Feature> listOfFeaturesPV2 = new ArrayList<Feature>();
		listOfFeaturesPV1 = PV1.getValuation().getFeature();
		listOfFeaturesPV2 = PV2.getValuation().getFeature();
		
		Feature leg1Feature1 = listOfFeaturesPV1.get(0);
		Feature leg1Feature2 = listOfFeaturesPV1.get(1);
		Feature leg1Feature3 = listOfFeaturesPV1.get(2);
		Feature leg1Feature4 = listOfFeaturesPV1.get(3);
		
		Feature leg2Feature1 = listOfFeaturesPV2.get(0);
		Feature leg2Feature2 = listOfFeaturesPV2.get(1);
		Feature leg2Feature3 = listOfFeaturesPV2.get(2);
		
		verifyXMLValueAgainstData("currency leg1Feature1 ", leg1Feature1.getCashflow().getCurrency(), "EUR", results);
		verifyXMLValueAgainstData("currency leg1Feature2 ", leg1Feature2.getCashflow().getCurrency(), "CAD", results);
		verifyXMLValueAgainstData("currency leg1Feature3 ", leg1Feature3.getCashflow().getCurrency(), "EUR", results);
		verifyXMLValueAgainstData("currency leg1Feature4 ", leg1Feature4.getCashflow().getCurrency(), "EUR", results);
		verifyXMLValueAgainstData("currency leg2Feature1 ", leg2Feature1.getCashflow().getCurrency(), "EUR", results);
		verifyXMLValueAgainstData("currency leg2Feature2 ", leg2Feature2.getCashflow().getCurrency(), "USD", results);
		verifyXMLValueAgainstData("currency leg2Feature3 ", leg2Feature3.getCashflow().getCurrency(), "UAH", results);
		
		updateReportWithTestResults(results);
		updateReportToFinal(reportFile, results, startTime);
		System.out.println("featureGenerationTestS test finished");
	}
	
	@Test
	public void productGenerationTest() {
		Common.print("productGenerationTest test started");
		Map<String, String> results = new LinkedHashMap<String, String>();
		downloadFromUnixToLocal(props.getProperty("TADSFOLDERONUNIX"));
		listOfXmlObjects = getTaMessages(localPath, results);
		mapOfXmlObjects = getCompareMapOfXmlObjects();
		TaMessage PV1 = mapOfXmlObjects.get(jsonForTest.getTraderBook().get(0)
				.getTrades().get(0).getTradeId());
		TaMessage PV2 = mapOfXmlObjects.get(jsonForTest.getTraderBook().get(0)
				.getTrades().get(1).getTradeId());
		TaMessage PV3 = mapOfXmlObjects.get(jsonForTest.getTraderBook().get(0)
				.getTrades().get(2).getTradeId());
		TaMessage PV4 = mapOfXmlObjects.get(jsonForTest.getTraderBook().get(0)
				.getTrades().get(3).getTradeId());
		TaMessage PV5 = mapOfXmlObjects.get(jsonForTest.getTraderBook().get(0)
				.getTrades().get(4).getTradeId());

		Trades tradePV1 = jsonForTest.getTraderBook().get(0).getTrades().get(0);
		Trades tradePV2 = jsonForTest.getTraderBook().get(0).getTrades().get(1);
		Trades tradePV3 = jsonForTest.getTraderBook().get(0).getTrades().get(2);
		Trades tradePV4 = jsonForTest.getTraderBook().get(0).getTrades().get(3);
		Trades tradePV5 = jsonForTest.getTraderBook().get(0).getTrades().get(4);
		// step 3
		if (mapOfXmlObjects.size() == 5) {
			results.put("Check that PV#6 was filtered out", "PASS");
		} else
			results.put("Check that PV#6 was filtered out", "FAIL");
		// step 4
		verifyXMLValueAgainstData(
				"Verify \"product_id\" attribute is populated correctly for PV#1.",
				PV1.getValuation().getProduct().getProductId(),
				"CreditDefaultSwap", results);
		verifyXMLValueAgainstData(
				"Verify \"product_id\" attribute is populated correctly for PV#2.",
				PV2.getValuation().getProduct().getProductId(),
				"CreditDefaultSwap", results);
		verifyXMLValueAgainstData(
				"Verify \"product_id\" attribute is populated correctly for PV#3.",
				PV3.getValuation().getProduct().getProductId(),
				"CreditDefaultSwap", results);
		verifyXMLValueAgainstData(
				"Verify \"product_id\" attribute is populated correctly for PV#4.",
				PV4.getValuation().getProduct().getProductId(),
				"CreditDefaultSwap", results);
		verifyXMLValueAgainstData(
				"Verify \"product_id\" attribute is populated correctly for PV#5.",
				PV5.getValuation().getProduct().getProductId(), "BondOption",
				results);
		// step 5
		verifyXMLValueAgainstData(
				"Verify \"front_office_product_type\"  populated correctly for PV#1.",
				PV1.getValuation().getProduct().getFrontOfficeProductType(),
				tradePV1.getPRDSProductType(), results);
		verifyXMLValueAgainstData(
				"Verify \"front_office_product_type\"  populated correctly for PV#2.",
				PV2.getValuation().getProduct().getFrontOfficeProductType(),
				tradePV2.getPRDSProductType(), results);
		verifyXMLValueAgainstData(
				"Verify \"front_office_product_type\"  populated correctly for PV#3.",
				PV3.getValuation().getProduct().getFrontOfficeProductType(),
				tradePV3.getPRDSProductType(), results);
		verifyXMLValueAgainstData(
				"Verify \"front_office_product_type\"  populated correctly for PV#4.",
				PV4.getValuation().getProduct().getFrontOfficeProductType(),
				tradePV4.getPRDSProductType(), results);
		verifyXMLValueAgainstData(
				"Verify \"front_office_product_type\"  populated correctly for PV#5.",
				PV5.getValuation().getProduct().getFrontOfficeProductType(),
				tradePV5.getFinancialProductTypeId(), results);
		// step 6
		verifyXMLValueAgainstData(
				"Verify that \"front_office_product_subtype\" attribute is populated correctly for PV#1",
				PV1.getValuation().getProduct().getFrontOfficeProductSubtype(),
				tradePV1.getFinancialProductTypeId(), results);
		verifyXMLValueAgainstData(
				"Verify that \"front_office_product_subtype\" attribute is populated correctly for PV#4",
				PV4.getValuation().getProduct().getFrontOfficeProductSubtype(),
				tradePV4.getFinancialProductTypeId(), results);
		verifyXMLValueAgainstData(
				"Verify that \"front_office_product_subtype\" attribute is populated correctly for PV#5",
				PV5.getValuation().getProduct().getFrontOfficeProductSubtype(),
				tradePV5.getFinancialProductTypeId(), results);
		verifyXMLValueAgainstData(
				"Verify that \"front_office_product_subtype\" attribute is populated correctly for PV#2",
				PV2.getValuation().getProduct().getFrontOfficeProductSubtype(),
				"", results);
		verifyXMLValueAgainstData(
				"Verify that \"front_office_product_subtype\" attribute is populated correctly for PV#3",
				PV3.getValuation().getProduct().getFrontOfficeProductSubtype(),
				"", results);
		// step 7
		for (Entry<String, TaMessage> entry : mapOfXmlObjects.entrySet()) {

			verifyXMLValueAgainstData(
					"Verify \"product_source\" attribute is populated correctly for all PVs.",
					entry.getValue().getValuation().getProduct()
							.getProductSource(), "ACS", results);
		}

		updateReportWithTestResults(results);
		updateReportToFinal(reportFile, results, startTime);
		System.out.println("productGenerationTest test finished");
	}
	
	@Test
	public void headerGenerationTest() {
		Common.print("headerGenerationTest test started");
		Map<String, String> results = new LinkedHashMap<String, String>();
		downloadFromUnixToLocal(props.getProperty("TADSFOLDERONUNIX"));
		listOfXmlObjects = getTaMessages(localPath, results);
		/*StringBuilder businesObjIdFromJson = new StringBuilder();
		businesObjIdFromJson.append(jsonForTest.get)*/
		String businesObjIdFromJson =  getAccountableIdFromJsonForNY(jsonForTest, 1);
		//Book_Name/front_office_prod_type/ctpy_id/trade_id/
		MessageHeader header = listOfXmlObjects.get(0).getHeader();
		verifyXMLValueAgainstData("Verify \"business_object_id\" attribute is populated correctly.", header.getBusinessObjectId(),businesObjIdFromJson ,results);
		verifyXMLValueAgainstData("Verify \"business_object_type\" attribute is populated correctly.", header.getBusinessObjectType(), "VALUATION", results);
		verifyXMLValueAgainstData("Verify \"business_object_owner\" attribute is populated correctly.", header.getBusinessObjectOwner(), "ODS", results);
		verifyXMLValueAgainstData("Verify \"business_object_version\" attribute is populated correctly.", String.valueOf(header.getBusinessObjectVersion()), jsonForTest.getDatasetRankedVersion(), results);
		verifyXMLValueAgainstData("Verify \"business_event_type\" attribute is populated correctly.", header.getBusinessEventType(), "NEW", results);
		verifyXMLValueAgainstData("Verify \"business_event_timestamp\" attribute is populated correctly.", header.getBusinessEventTimestamp().toString().substring(0, 10), jsonForTest.getAsAtTimeStamp().substring(0, 10), results);
		verifyXMLValueAgainstData("Verify \"delivery_unit_instance_id\" attribute is populated correctly.", String.valueOf(header.getDeliveryUnitInstanceId()), getDeliveryUnitInstanceIdFromDataBase(), results);
		verifyXMLValueAgainstData("Verify \"delivery_unit_instance_entity_id\" attribute is populated correctly.", header.getDeliveryUnitInstanceEntityId(), jsonForTest.getTraderBook().get(0).getTraderBookCode(), results);
		verifyXMLValueAgainstData("Check that delivery_unit_id atribute is populated with value \"18\" for OD NY LOB", String.valueOf(header.getDeliveryUnitId()), "18", results);
		
		updateReportWithTestResults(results);
		updateReportToFinal(reportFile, results, startTime);
		System.out.println("headerGenerationTest test finished");
	}
	
	@Test
	public void tradeEntityMultilegGeneration() {
		Common.print("tradeEntityMultilegGeneration test started");
		Map<String, String> results = new LinkedHashMap<String, String>();
		downloadFromUnixToLocal(props.getProperty("TADSFOLDERONUNIX"));
		listOfXmlObjects = getTaMessages(localPath, results);
		mapOfXmlObjects = getCompareMapOfXmlObjects();
		TaMessage PV1 = mapOfXmlObjects.get(jsonForTest.getTraderBook().get(0)
				.getTrades().get(0).getTradeId());
		TaMessage PV2 = mapOfXmlObjects.get(jsonForTest.getTraderBook().get(0)
				.getTrades().get(1).getTradeId());
		if(PV1!=null){
		Trades tradePV1 = jsonForTest.getTraderBook().get(0).getTrades().get(0);
		Trades tradePV2 = jsonForTest.getTraderBook().get(0).getTrades().get(1);
		//spep 4
		
		if(PV1.getValuation().getFeature().size()==2){
			results.put("TADS for PV#1 has 2 feature.", "PASS");
		} else results.put("TADS for PV#1 has 2 feature.", "FAIL");
		
		if(PV2.getValuation().getFeature().size()==3){
			results.put("TADS for PV#2 has 3 feature", "PASS");
		} else results.put("TADS for PV#2 has 3 feature", "FAIL");

		//step 5
		verifyXMLValueAgainstData("Verify \"type\" attribute is populated correctly PV1 feature 1.", PV1.getValuation().getFeature().get(0).getType(), "PV", results);
		verifyXMLValueAgainstData("Verify \"type\" attribute is populated correctly PV1 feature 2.", PV1.getValuation().getFeature().get(1).getType(), "PV", results);
		verifyXMLValueAgainstData("Verify \"type\" attribute is populated correctly PV2 feature 1.", PV2.getValuation().getFeature().get(0).getType(), "PV", results);
		verifyXMLValueAgainstData("Verify \"type\" attribute is populated correctly PV2 feature 2.", PV2.getValuation().getFeature().get(1).getType(), "PV", results);
		verifyXMLValueAgainstData("Verify \"type\" attribute is populated correctly PV2 feature 3.", PV2.getValuation().getFeature().get(2).getType(), "PV", results);
		//step 6
		if(tradePV1.getBuySellCode().equals("S")){
			
			verifyXMLValueAgainstData("Verify \"direction\" attribute is populated correctly PV1 feature 1.", PV1.getValuation().getFeature().get(0).getDirection(), "R", results);
			verifyXMLValueAgainstData("Verify \"direction\" attribute is populated correctly PV1 feature 2.", PV1.getValuation().getFeature().get(1).getDirection(), "R", results);
			verifyXMLValueAgainstData("Verify \"direction\" attribute is populated correctly PV2 feature 1.", PV2.getValuation().getFeature().get(0).getDirection(), "R", results);
			verifyXMLValueAgainstData("Verify \"direction\" attribute is populated correctly PV2 feature 2.", PV2.getValuation().getFeature().get(1).getDirection(), "R", results);
			verifyXMLValueAgainstData("Verify \"direction\" attribute is populated correctly PV2 feature 3.", PV2.getValuation().getFeature().get(2).getDirection(), "R", results);
		} else  
		{
			verifyXMLValueAgainstData("Verify \"direction\" attribute is populated correctly PV1 feature 1.", PV1.getValuation().getFeature().get(0).getDirection(), "P", results);
			verifyXMLValueAgainstData("Verify \"direction\" attribute is populated correctly PV1 feature 2.", PV1.getValuation().getFeature().get(1).getDirection(), "P", results);
			verifyXMLValueAgainstData("Verify \"direction\" attribute is populated correctly PV2 feature 1.", PV2.getValuation().getFeature().get(0).getDirection(), "P", results);
			verifyXMLValueAgainstData("Verify \"direction\" attribute is populated correctly PV2 feature 2.", PV2.getValuation().getFeature().get(1).getDirection(), "P", results);
			verifyXMLValueAgainstData("Verify \"direction\" attribute is populated correctly PV2 feature 3.", PV2.getValuation().getFeature().get(2).getDirection(), "P", results);
		}
		}
		else {
			results.put("PV is null", "Assertion Error");
		}
		
		updateReportWithTestResults(results);
		updateReportToFinal(reportFile, results, startTime);
		System.out.println("tradeEntityMultilegGeneration test finished");
	}
	
	@Test
	public void tradeEntityGeneration(){
		Common.print("tradeEntityGeneration test started");
		Map<String, String> results = new LinkedHashMap<String, String>();
		downloadFromUnixToLocal(props.getProperty("TADSFOLDERONUNIX"));
		listOfXmlObjects = getTaMessages(localPath, results);
		mapOfXmlObjects = getCompareMapOfXmlObjects();
		TaMessage PV1 = mapOfXmlObjects.get(jsonForTest.getTraderBook().get(0)
				.getTrades().get(0).getTradeId());
		TaMessage PV2 = mapOfXmlObjects.get(jsonForTest.getTraderBook().get(0)
				.getTrades().get(1).getTradeId());
		
		Trades tradePV1 = jsonForTest.getTraderBook().get(0).getTrades().get(0);
		Trades tradePV2 = jsonForTest.getTraderBook().get(0).getTrades().get(1);
		TradeIdentity tradeIndentityPV1 = PV1.getValuation().getTradeIdentity();
		TradeIdentity tradeIndentityPV2 = PV2.getValuation().getTradeIdentity();
		
		
		//step 5 
		verifyXMLValueAgainstData("Verify \"id\" attribute is populated correctly for PV1.", tradeIndentityPV1.getId(), tradePV1.getTradeId(), results);
		verifyXMLValueAgainstData("Verify \"id\" attribute is populated correctly for PV2.", tradeIndentityPV2.getId(), tradePV2.getTradeId(), results);
		//step 6
		verifyXMLValueAgainstData("Verify \"group_id\" attribute is populated correctly for PV#1.", tradeIndentityPV1.getGroupId(), tradePV1.getStructureId(), results);
		verifyXMLValueAgainstData("Verify \"group_id\" attribute is populated correctly for PV#2.", tradeIndentityPV2.getGroupId(), tradePV2.getStructureId(), results);
		//step 7
		verifyXMLValueAgainstData("Verify \"source_system\" attribute is populated correctly for PV#1.", tradeIndentityPV1.getSourceSystem(), tradePV1.getTradeSourceSystemName(), results);
		verifyXMLValueAgainstData("Verify \"source_system\" attribute is populated correctly for PV#2.", tradeIndentityPV2.getSourceSystem(), tradePV2.getTradeSourceSystemName(), results);
		//step 8
		verifyXMLValueAgainstData("Verify \"version\" attribute is populated correctly for PV#1.", tradeIndentityPV1.getVersion(), tradePV1.getTradeVersion(), results);
		verifyXMLValueAgainstData("Verify \"version\" attribute is populated correctly for PV#2.", tradeIndentityPV2.getVersion(), null, results);
		
		updateReportWithTestResults(results);
		updateReportToFinal(reportFile, results, startTime);
		System.out.println("tradeEntityGeneration test finished");
		
	}
	
	@Test
	public void valuationGeneration(){
		Common.print("valuationGeneration test started");
		Map<String, String> results = new LinkedHashMap<String, String>();
		downloadFromUnixToLocal(props.getProperty("TADSFOLDERONUNIX"));
		listOfXmlObjects = getTaMessages(localPath, results);
		mapOfXmlObjects = getCompareMapOfXmlObjects();
		TaMessage PV1 = mapOfXmlObjects.get(jsonForTest.getTraderBook().get(0)
				.getTrades().get(0).getTradeId());
		TaMessage PV2 = mapOfXmlObjects.get(jsonForTest.getTraderBook().get(0)
				.getTrades().get(1).getTradeId());
		
		Trades tradePV1 = jsonForTest.getTraderBook().get(0).getTrades().get(0);
		Trades tradePV2 = jsonForTest.getTraderBook().get(0).getTrades().get(1);
		
		
		
		//step 4
		verifyXMLValueAgainstData("Verify \"accountable_id\" attribute is populated correctly for PV#1", PV1.getValuation().getAccountableId(), getAccountableIdFromJsonForNY(jsonForTest, 1), results);
		verifyXMLValueAgainstData("Verify \"accountable_id\" attribute is populated correctly for PV#2", PV2.getValuation().getAccountableId(), getAccountableIdFromJsonForNY(jsonForTest, 2), results);
		//step 5
		verifyXMLValueAgainstData("Verify \"version\" attribute is populated correctly for PV#1.", String.valueOf(PV1.getValuation().getVersion()), jsonForTest.getDatasetRankedVersion(), results);
		verifyXMLValueAgainstData("Verify \"version\" attribute is populated correctly for PV#2", String.valueOf(PV2.getValuation().getVersion()) , jsonForTest.getDatasetRankedVersion(), results);
		//step 6
		verifyXMLValueAgainstData("Verify \"source_system\" attribute is populated correctly for PV#1.", PV1.getValuation().getSourceSystem(), jsonForTest.getValuationSource(), results);
		verifyXMLValueAgainstData("Verify \"source_system\" attribute is populated correctly for PV#2.", PV2.getValuation().getSourceSystem(), jsonForTest.getValuationSource(), results);
		//step 7
		verifyXMLValueAgainstData("Verify \"accountable_type\" attribute is populated correctly for PV#1.", PV1.getValuation().getAccountableType(), "PV", results);
		verifyXMLValueAgainstData("Verify \"accountable_type\" attribute is populated correctly for PV#2.", PV2.getValuation().getAccountableType(), "PV", results);
		//step 8
		verifyXMLValueAgainstData("Verify \"internal_version\" attribute is populated correctly for PV#1.", PV1.getValuation().getInternalVersion(), "1", results);
		verifyXMLValueAgainstData("Verify \"internal_version\" attribute is populated correctly for PV#2.", PV2.getValuation().getInternalVersion(), "1", results);
		//step 9
		verifyXMLValueAgainstData("Verify \"contract_maturity_date\" attribute is populated correctly for PV#1.", PV1.getValuation().getContractMaturityDate().toString().substring(0, 10), tradePV1.getMaturityDate().substring(0, 10), results);
		verifyXMLValueAgainstData("Verify \"contract_maturity_date\" attribute is populated correctly for PV#2.", PV2.getValuation().getContractMaturityDate().toString().substring(0, 10), tradePV2.getMaturityDate().substring(0, 10), results);
		//step 11
		verifyXMLValueAgainstData("Verify \"valuation_date\" attribute is populated correctly for PV#1.", PV1.getValuation().getValuationDate().toString().substring(0, 10), jsonForTest.getCOBDate().substring(0, 10), results);
		verifyXMLValueAgainstData("Verify \"valuation_date\" attribute is populated correctly for PV#2.", PV2.getValuation().getValuationDate().toString().substring(0, 10), jsonForTest.getCOBDate().substring(0, 10), results);
		
		updateReportWithTestResults(results);
		updateReportToFinal(reportFile, results, startTime);
		System.out.println("valuationGeneration test finished");
		
	}
}
