
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.bind.JAXBException;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import DataSourcingPack.PnL_Credit.JsonObjectClasses.FormJsonSource;
import DataSourcingPack.PnL_Credit.JsonObjectClasses.Measures;
import DataSourcingPack.PnL_Credit.JsonObjectClasses.PnLReport;
import DataSourcingPack.PnL_Credit.JsonObjectClasses.TradeLegs;
import DataSourcingPack.PnL_Credit.JsonObjectClasses.Trades;
import UtilsPack.Common;
import UtilsPack.Report;
import UtilsPack.TemplateActions;
import UtilsPack.TimeCounter;
import UtilsPack.UtilsSQL;
import UtilsPack.WorkWithProperties;

/**
 * Class that realizes classic assertion-style test approach for validate tads generation.
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
public class LDNTadsMessageGenerationTest extends TestHelper {
	
	public static final String COMMONTESTFOLDER = "/TADS-message Generation London/";
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
				+ DateFormat.format(reportDate)+"_LDN_generation" + ".xlsx");
		startTime = TimeCounter.getCurrentTime();
		
		String result = "";
		String commandToRunForClean = "cd %%UNIX_PATH_DATA%%; rm *.json; ls; ";
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
			props.put("FEEDID", "7"); // 7 LONDON 18 NY
			props.put("ORGLSRCID", "CREDIT");
			props.put("FEEDENTITYID", "DBNY_CAP_LCH");
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
		//run GetValuations for nedded workflowId and get files to local
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
	public void bookGenerationTest()  throws JAXBException{
		System.out.println("bookGenerationTest test started");
		Map<String, String> results = new LinkedHashMap<String, String>();
		downloadFromUnixToLocal(props.getProperty("TADSFOLDERONUNIX"));
		listOfXmlObjects = getTaMessages(localPath, results);
		mapOfXmlObjects = getCompareMapOfXmlObjects();
		Book book = listOfXmlObjects.get(0).getValuation().getBook();
		Trades trades = jsonForTest.getTraderBook().get(0).getTrades().get(0);
		
		verifyXMLValueAgainstData("Verify book Name", book.getName(),
				jsonForTest.getTraderBook().get(0).getTraderBookCode(), results);
		verifyXMLValueAgainstData("Verify book Source system",
				book.getSourceSystem(), jsonForTest.getTraderBook().get(0)
						.getTraderBookSourceSystemName(), results);
		verifyXMLValueAgainstData("Verify \"capacity\" attribute is populated correctly.", book.getCapacity(), "PRINCIPAL", results);
		verifyXMLValueAgainstData("legal_entity/source_system - \"AMI_NET\"", book.getLegalEntity().getSourceSystem(), "AMI_NET", results);
		verifyXMLValueAgainstData("legal_entity/type -   \"AMI_CODE\"", book.getLegalEntity().getType(),"AMI_CODE", results);
		
		
		// against RDBook
		if (getRDBook(jsonForTest)!=null){
			verifyAgainstRDbook("organization_unit_id is populated from RD_BOOK.BOOK_NAME", book.getOrganizationUnitId(), String.valueOf(getRDBook(jsonForTest).getTRADERBOOKNAME()), results);
			verifyAgainstRDbook("organization_unit_source_system is populated from  RD_BOOK.TRADER_BK_SRC_SYS", book.getOrganizationUnitSourceSystem(), getRDBook(jsonForTest).getTRADERBKSRCSYS(), results);
			verifyAgainstRDbook("Verify legal_entity/id - value from <LE_CODE> from book static data", book.getLegalEntity().getId(), getRDBook(jsonForTest).getLECODE(), results);
			
			
		}else {
			results.put("RDBook creation", "FAIL");
		}
		updateReportWithTestResults(results);
		updateReportToFinal(reportFile, results, startTime);
		System.out.println("bookGenerationTest test finished");
	}
	
	@Test
	public void cashflowGenerationTest() {
		downloadFromUnixToLocal(props.getProperty("TADSFOLDERONUNIX"));
		System.out.println("cashFlowGenerationTest started");
		Map<String, String> results = new LinkedHashMap<String, String>();
		listOfXmlObjects = getTaMessages(localPath, results);
		mapOfXmlObjects = getCompareMapOfXmlObjects();
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
	public void counterpartyGenerationTest(){
		Common.print("counterpartyGenerationTest test started");
		downloadFromUnixToLocal(props.getProperty("TADSFOLDERONUNIX"));
		Map<String, String> results = new LinkedHashMap<String, String>();
		
		listOfXmlObjects = getTaMessages(localPath, results);
		mapOfXmlObjects = getCompareMapOfXmlObjects();
		
		TaMessage PV1 = mapOfXmlObjects.get(jsonForTest.getTraderBook().get(0)
				.getTrades().get(0).getTradeId());
		TaMessage PV2 = mapOfXmlObjects.get(jsonForTest.getTraderBook().get(0)
				.getTrades().get(1).getTradeId());
		
		Counterparty counterpartyPV1 = PV1.getValuation().getCounterParty();
		Counterparty counterpartyPV2 = PV2.getValuation().getCounterParty();
		
		Trades trade1 = jsonForTest.getTraderBook().get(0).getTrades().get(0);
		Trades trade2 = jsonForTest.getTraderBook().get(0).getTrades().get(1);
		
		verifyXMLValueAgainstData("Verify counterparty ID for PV1", counterpartyPV1.getId(), trade1.getPartyId(), results);
		verifyXMLValueAgainstData("Verify counterparty ID for PV2", counterpartyPV2.getId(), trade2.getPartyId(), results);
		verifyXMLValueAgainstData("Verify counterparty source system for PV1", counterpartyPV1.getSourceSystem(), trade1.getPartyIdSourceSystem() , results);
		verifyXMLValueAgainstData("Verify counterparty source system for PV2", counterpartyPV2.getSourceSystem(), trade2.getPartyIdSourceSystem(), results);
		verifyXMLValueAgainstData("Verify counterparty type for PV1", counterpartyPV1.getType(), "L_CPTY_ID", results);
		verifyXMLValueAgainstData("Verify counterparty type for PV2", counterpartyPV2.getType(), "L_CPTY_ID", results);
		verifyXMLValueAgainstData("Verify counterparty role for PV1", counterpartyPV1.getRole(), "INTERNAL", results);
		verifyXMLValueAgainstData("Verify counterparty role for PV2", counterpartyPV2.getRole(), "EXTERNAL", results);
		
		updateReportWithTestResults(results);
		updateReportToFinal(reportFile, results, startTime);
		System.out.println("counterpartyGenerationTest test finished");
	}
	
	@Test
	public void featureGenerationTest() {
		Common.print("featureGenerationTest test started");
		downloadFromUnixToLocal(props.getProperty("TADSFOLDERONUNIX"));
		Map<String, String> results = new LinkedHashMap<String, String>();
		
		listOfXmlObjects = getTaMessages(localPath, results);
		mapOfXmlObjects = getCompareMapOfXmlObjects();
		
		TaMessage PV1 = mapOfXmlObjects.get(jsonForTest.getTraderBook().get(0)
				.getTrades().get(0).getTradeId());
		TaMessage PV2 = mapOfXmlObjects.get(jsonForTest.getTraderBook().get(0)
				.getTrades().get(1).getTradeId());
		
		Feature featurePV1 = PV1.getValuation().getFeature().get(0);
		Feature featurePV2 = PV2.getValuation().getFeature().get(0);
		
		
		verifyXMLValueAgainstData("Verify feature type PV1", featurePV1.getType(),
				"PV", results);
		verifyXMLValueAgainstData("Verify feature type PV2", featurePV2.getType(),
				"PV", results);
		verifyXMLValueAgainstData("Verify \"direction\" attribute is populated correctly for PV1.", featurePV1.getDirection(), "P", results);
		verifyXMLValueAgainstData("Verify \"direction\" attribute is populated correctly for PV2.", featurePV2.getDirection(), "R", results);
		 
		updateReportWithTestResults(results);
		updateReportToFinal(reportFile, results, startTime);
		System.out.println("featureGenerationTest test finished");
	}
	
	@Test
	public void headerGenerationTest() {
		Common.print("headerGenerationTest test started");
		downloadFromUnixToLocal(props.getProperty("TADSFOLDERONUNIX"));
		Map<String, String> results = new LinkedHashMap<String, String>();
		
		listOfXmlObjects = getTaMessages(localPath, results);
		mapOfXmlObjects = getCompareMapOfXmlObjects();
		
		String businesObjIdFromJson =  getAccountableIdFromJsonForLondon(jsonForTest, 1);
		MessageHeader header = listOfXmlObjects.get(0).getHeader();
		
		verifyXMLValueAgainstData("Verify \"business_object_id\" attribute is populated correctly.", header.getBusinessObjectId(),businesObjIdFromJson ,results);
		verifyXMLValueAgainstData("Verify \"business_object_type\" attribute is populated correctly.", header.getBusinessObjectType(), "VALUATION", results);
		verifyXMLValueAgainstData("Verify \"business_object_owner\" attribute is populated correctly.", header.getBusinessObjectOwner(), "ODS", results);
		verifyXMLValueAgainstData("Verify \"business_object_version\" attribute is populated correctly.", String.valueOf(header.getBusinessObjectVersion()), jsonForTest.getDatasetRankedVersion(), results);
		verifyXMLValueAgainstData("Verify \"business_event_type\" attribute is populated correctly.", header.getBusinessEventType(), "NEW", results);
		verifyXMLValueAgainstData("Verify \"business_event_timestamp\" attribute is populated correctly.", header.getBusinessEventTimestamp().toString().substring(0, 10), jsonForTest.getAsAtTimeStamp().substring(0, 10), results);
		verifyXMLValueAgainstData("Verify \"delivery_unit_instance_id\" attribute is populated correctly.", String.valueOf(header.getDeliveryUnitInstanceId()), getDeliveryUnitInstanceIdFromDataBase(), results);
		verifyXMLValueAgainstData("Verify \"delivery_unit_instance_entity_id\" attribute is populated correctly.", header.getDeliveryUnitInstanceEntityId(), jsonForTest.getTraderBook().get(0).getTraderBookCode() + "/" + jsonForTest.getTraderBook().get(0).getTraderBookSourceSystemName(), results);
		verifyXMLValueAgainstData("Check that delivery_unit_id atribute is populated with value \"7\" for OD NY LOB", String.valueOf(header.getDeliveryUnitId()), "7", results);
		
		updateReportWithTestResults(results);
		updateReportToFinal(reportFile, results, startTime);
		System.out.println("headerGenerationTest test finished");
	}
	
	@Test
	public void multiLegPVsGenerationTest() {
		Common.print("multiLegPVsGenerationTest test started");
		downloadFromUnixToLocal(props.getProperty("TADSFOLDERONUNIX"));
		Map<String, String> results = new LinkedHashMap<String, String>();
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
		
		Trades trade1 = jsonForTest.getTraderBook().get(0).getTrades().get(0);
		Trades trade2 = jsonForTest.getTraderBook().get(0).getTrades().get(1);
		
		String acc_idPV1 = getAccountableIdFromJsonForLondon(jsonForTest, 1);
		String acc_idPV2 = getAccountableIdFromJsonForLondon(jsonForTest, 2);
		//step 3
		verifyXMLValueAgainstData("Verify accountable_id elment for PV1", PV1.getValuation().getAccountableId(), acc_idPV1, results);
		verifyXMLValueAgainstData("Verify accountable_id elment for PV2", PV2.getValuation().getAccountableId(), acc_idPV2, results);
		//step 4
		if (PV1.getValuation().getFeature().size()==6){
			results.put("TADS for PV1 has 6 feature.", "PASS");
		} else results.put("TADS for PV#1 has 6 feature.", "FAIL");
		if(PV2.getValuation().getFeature().size()==1){
			results.put("TADS for PV2 has 1 feature", "PASS");
		} else results.put("TADS for PV2 has 1 feature", "FAIL");
		//step 5
		//String xml_leg_id = PV1.getValuation().getFeature().get(0).getCashflow().getLegId();
		//String json_leg_id = trade1.getTradeLegs().get(0).getTradeLegID();
		
		//Verify that each  <leg_id> from TADS has corresponding <amount>  and <currency> elements inside <feature>
		results.put("Check TADS-message for PV#1", "");
		for(Feature feature : listOfFeaturesPV1){
			//step 6 for pv1
			verifyXMLValueAgainstData("Verify \"type\" attribute is populated correctly for PV1", feature.getType(), "PV", results);
			//step 7 for pv1
			if(trade1.getBuySellCode().equals("S")){
				verifyXMLValueAgainstData("Verify \"direction\" attribute is populated correctly for PV1", feature.getDirection(), "R", results);
			} else verifyXMLValueAgainstData("Verify \"direction\" attribute is populated correctly for PV1", feature.getDirection(), "P", results);
			// step 5 for pv1
			for(TradeLegs leg: trade1.getTradeLegs()){
				if(feature.getCashflow().getLegId().equals(leg.getTradeLegID())){
					for(Measures measure : leg.getMeasures()){
						if(measure.equals("TXC")){
							
							verifyXMLValueAgainstData("Verify amount for leg id " + leg.getTradeLegID(), 
									String.valueOf(feature.getCashflow().getAmount()),
									measure.getAmount(), results);
							verifyXMLValueAgainstData("Verify currency for leg id " + leg.getTradeLegID(),
									feature.getCashflow().getCurrency(), measure.getCurrencyCode(), results);
						}
					}
				}
			}
		}
		results.put("Check TADS-message for PV#2", "");
		for(Feature feature : listOfFeaturesPV2){
			//step 6 fir pv2
			verifyXMLValueAgainstData("Verify \"type\" attribute is populated correctly for PV2", feature.getType(), "PV", results);
			//step 7 for pv2
			if(trade2.getBuySellCode().equals("S")){
				verifyXMLValueAgainstData("Verify \"direction\" attribute is populated correctly for PV2", feature.getDirection(), "R", results);
			} else verifyXMLValueAgainstData("Verify \"direction\" attribute is populated correctly for PV2", feature.getDirection(), "P", results);
			//step 5 for pv1
			for(TradeLegs leg : trade2.getTradeLegs()){
				if(feature.getCashflow().getLegId().equals(leg.getTradeLegID())){
					for(Measures measure : leg.getMeasures()){
						if(measure.equals("TXC")){
							verifyXMLValueAgainstData("Verify amount for leg id " + leg.getTradeLegID(), 
									String.valueOf(feature.getCashflow().getAmount()),
									measure.getAmount(), results);
							verifyXMLValueAgainstData("Verify currency for leg id " + leg.getTradeLegID(),
									feature.getCashflow().getCurrency(), measure.getCurrencyCode(), results);
						}
					}
				}
			}
		}
		
		updateReportWithTestResults(results);
		updateReportToFinal(reportFile, results, startTime);
		System.out.println("multiLegPVsGenerationTest test finished");
	}

	@Test
	public void productGenerationTest() {
		Common.print("productGenerationTest test started");
		downloadFromUnixToLocal(props.getProperty("TADSFOLDERONUNIX"));
		Map<String, String> results = new LinkedHashMap<String, String>();
		listOfXmlObjects = getTaMessages(localPath, results);
		mapOfXmlObjects = getCompareMapOfXmlObjects();
		TaMessage PV1 = mapOfXmlObjects.get(jsonForTest.getTraderBook().get(0)
				.getTrades().get(0).getTradeId());
		TaMessage PV2 = mapOfXmlObjects.get(jsonForTest.getTraderBook().get(0)
				.getTrades().get(1).getTradeId());
		Trades tradePV1 = jsonForTest.getTraderBook().get(0).getTrades().get(0);
		Trades tradePV2 = jsonForTest.getTraderBook().get(0).getTrades().get(1);
		//step 5
		verifyXMLValueAgainstData(
				"Verify \"product_id\" attribute is populated correctly for PV#1.",
				PV1.getValuation().getProduct().getProductId(),
				"IRSwaption", results);
		verifyXMLValueAgainstData(
				"Verify \"product_id\" attribute is populated correctly for PV#2.",
				PV2.getValuation().getProduct().getProductId(),
				"CreditDefaultSwap", results);
		//step 6
		verifyXMLValueAgainstData(
				"Verify \"front_office_product_type\"  populated correctly for PV#1.",
				PV1.getValuation().getProduct().getFrontOfficeProductType(),
				tradePV1.getPRDSProductType(), results);
		
		verifyXMLValueAgainstData(
				"Verify \"front_office_product_type\"  populated correctly for PV#2.",
				PV2.getValuation().getProduct().getFrontOfficeProductType(),
				tradePV2.getFinancialProductTypeId(), results);
		//step 7
		verifyXMLValueAgainstData(
				"Verify that \"front_office_product_subtype\" attribute is populated correctly for PV#2",
				PV2.getValuation().getProduct().getFrontOfficeProductSubtype(),
				tradePV2.getFinancialProductTypeId(), results);
		verifyXMLValueAgainstData(
				"Verify that \"front_office_product_subtype\" attribute is populated correctly for PV#1",
				PV1.getValuation().getProduct().getFrontOfficeProductSubtype(),
				"", results);
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
	public void tradeIdentityGenerationTest() {
		Common.print("tradeIdentityGenerationTest test started");
		downloadFromUnixToLocal(props.getProperty("TADSFOLDERONUNIX"));
		Map<String, String> results = new LinkedHashMap<String, String>();
		listOfXmlObjects = getTaMessages(localPath, results);
		TradeIdentity tradeIdentity = listOfXmlObjects.get(0).getValuation().getTradeIdentity();
		Trades trades = jsonForTest.getTraderBook().get(0).getTrades().get(0);
		
		verifyXMLValueAgainstData("Verify \"id\" attribute is populated correctly.", tradeIdentity.getId(), trades.getTradeId(), results);
		verifyXMLValueAgainstData("Verify \"group_id\" attribute is populated correctly.", tradeIdentity.getGroupId(), trades.getStructureId(), results);
		verifyXMLValueAgainstData("Verify \"source_system\" attribute is populated correctly.", tradeIdentity.getSourceSystem(), trades.getTradeSourceSystemName(), results);
		verifyXMLValueAgainstData("Verify \"version\" attribute is populated correctly.", tradeIdentity.getVersion(), trades.getTradeVersion(), results);
		
		updateReportWithTestResults(results);
		updateReportToFinal(reportFile, results, startTime);
		System.out.println("tradeIdentityGenerationTest test finished");
	}
	
	@Test
	public void valuationGenerationTest() {
		Common.print("valuationGeneration test started");
		downloadFromUnixToLocal(props.getProperty("TADSFOLDERONUNIX"));
		Map<String, String> results = new LinkedHashMap<String, String>();
		listOfXmlObjects = getTaMessages(localPath, results);
		Trades trade = jsonForTest.getTraderBook().get(0).getTrades().get(0);
		
		Valuation valuation  = listOfXmlObjects.get(0).getValuation();
		verifyXMLValueAgainstData("Verify \"accountable_id\" attribute is populated correctly for PV#1",valuation.getAccountableId(), getAccountableIdFromJsonForLondon(jsonForTest, 1), results);
		verifyXMLValueAgainstData("Verify \"version\" attribute is populated correctly for PV#1.", String.valueOf(valuation.getVersion()), jsonForTest.getDatasetRankedVersion(), results);
		verifyXMLValueAgainstData("Verify \"source_system\" attribute is populated correctly for PV#1.", valuation.getSourceSystem(), jsonForTest.getValuationSource(), results);
		verifyXMLValueAgainstData("Verify \"accountable_type\" attribute is populated correctly for PV#1.", valuation.getAccountableType(), "PV", results);
		verifyXMLValueAgainstData("Verify \"internal_version\" attribute is populated correctly for PV#1.", valuation.getInternalVersion(), "1", results);
		verifyXMLValueAgainstData("Verify \"contract_maturity_date\" attribute is populated correctly for PV#1.", valuation.getContractMaturityDate().toString().substring(0, 10), trade.getMaturityDate().substring(0, 10), results);
		verifyXMLValueAgainstData("Verify \"valuation_date\" attribute is populated correctly for PV#1.", valuation.getValuationDate().toString().substring(0, 10), jsonForTest.getCOBDate().substring(0, 10), results);
		
		updateReportWithTestResults(results);
		updateReportToFinal(reportFile, results, startTime);
		System.out.println("valuationGeneration test finished");
	}
	
}
