/**
 * Mule Anypoint Template
 *
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 */

package org.mule.templates.integration;

import static org.junit.Assert.assertEquals;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mule.MessageExchangePattern;
import org.mule.api.MuleEvent;
import org.mule.api.MuleException;
import org.mule.context.notification.NotificationException;
import org.mule.processor.chain.SubflowInterceptingChainLifecycleWrapper;
import org.mule.streaming.ConsumerIterator;
import org.mule.templates.utils.Employee;

import com.mulesoft.module.batch.BatchTestHelper;
import com.workday.hr.EmployeeGetType;
import com.workday.hr.EmployeeReferenceType;
import com.workday.hr.EmployeeType;
import com.workday.hr.ExternalIntegrationIDReferenceDataType;
import com.workday.hr.IDType;
import com.workday.staffing.EventClassificationSubcategoryObjectIDType;
import com.workday.staffing.EventClassificationSubcategoryObjectType;
import com.workday.staffing.TerminateEmployeeDataType;
import com.workday.staffing.TerminateEmployeeRequestType;
import com.workday.staffing.TerminateEventDataType;

/**
 * The objective of this class is to validate the correct behavior of the flows
 * for this Anypoint Template that make calls to external systems.
 */
public class BusinessLogicIT extends AbstractTemplateTestCase {

	private static final long TIMEOUT_MILLIS = 30000;
	private static final long DELAY_MILLIS = 500;
	private static final String PATH_TO_TEST_PROPERTIES = "./src/test/resources/mule.test.properties";
	private static final Logger log = LogManager.getLogger(BusinessLogicIT.class);
	
	protected static final int TIMEOUT_SEC = 60;
	private BatchTestHelper helper;
	
    private String EXT_ID, EMAIL = "bwillis@gmailtest.com";
	private String SFDC_ID, ACCOUNT_ID, CONTACT_ID;
	private Employee employee;
	private String TERMINATION_ID;
    
    @BeforeClass
    public static void beforeTestClass() {
        System.setProperty("poll.startDelayMillis", "8000");
        System.setProperty("poll.frequencyMillis", "30000");
        Date initialDate = new Date(System.currentTimeMillis() - 1000 * 60 * 3);
        Calendar cal = Calendar.getInstance();
        cal.setTime(initialDate);
        System.setProperty(
        		"watermark.default.expression", 
        		"#[groovy: new GregorianCalendar("
        				+ cal.get(Calendar.YEAR) + ","
        				+ cal.get(Calendar.MONTH) + ","
        				+ cal.get(Calendar.DAY_OF_MONTH) + ","
        				+ cal.get(Calendar.HOUR_OF_DAY) + ","
        				+ cal.get(Calendar.MINUTE) + ","
        				+ cal.get(Calendar.SECOND) + ") ]");
    }

    @Before
    public void setUp() throws Exception {
    	final Properties props = new Properties();
    	try {
    		props.load(new FileInputStream(PATH_TO_TEST_PROPERTIES));
    	} catch (Exception e) {
    	   log.error("Error occured while reading mule.test.properties", e);
    	} 
    	TERMINATION_ID = props.getProperty("wday.termination.id");
    	
    	helper = new BatchTestHelper(muleContext);
		stopFlowSchedulers(POLL_FLOW_NAME);
		registerListeners();
		
		createTestDataInSandBox();
    }

    @After
    public void tearDown() throws MuleException, Exception {
    	deleteTestDataFromSandBox();
    }
    
    private void registerListeners() throws NotificationException {
		muleContext.registerListener(pipelineListener);
	}
    
	private void createTestDataInSandBox() throws MuleException, Exception {
		SubflowInterceptingChainLifecycleWrapper flow = getSubFlow("hireEmployee");
		flow.initialise();
		log.info("creating a workday employee...");
		try {
			flow.process(getTestEvent(prepareNewHire(), MessageExchangePattern.REQUEST_RESPONSE));						
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
    
    private List<Object> prepareNewHire(){
		EXT_ID = "Bruce_" + System.currentTimeMillis();
		log.info("employee name: " + EXT_ID);
		employee = new Employee(EXT_ID, "Willis1", EMAIL, "650-232-2323", "999 Main St", "San Francisco", "CA", "94105", "US", "o7aHYfwG", 
				"2014-04-17-07:00", "2014-04-21-07:00", "QA Engineer", "San_Francisco_site", "Regular", "Full Time", "Salary", "USD", "140000", "Annual", "39905", "21440", EXT_ID);
		List<Object> list = new ArrayList<Object>();
		list.add(employee);
		return list;
	}
    
    @SuppressWarnings("unchecked")
	@Test
    public void testMainFlow() throws Exception {
		Thread.sleep(20000);
		runSchedulersOnce(POLL_FLOW_NAME);
		waitForPollToRun();
		helper.awaitJobTermination(TIMEOUT_MILLIS, DELAY_MILLIS);
		helper.assertJobWasSuccessful();	

    	SubflowInterceptingChainLifecycleWrapper flow = getSubFlow("getWorkdayEmployee");
		flow.initialise();
		MuleEvent response = flow.process(getTestEvent(getEmployee(), MessageExchangePattern.REQUEST_RESPONSE));			
		EmployeeType workerRes = (EmployeeType) response.getMessage().getPayload();
		log.info("worker id:" + workerRes.getEmployeeData().get(0).getEmployeeID());
		
    	flow = getSubFlow("retrieveCaseSFDC");
    	flow.initialise();
		
		ConsumerIterator<Map<String, Object>> iterator = (ConsumerIterator<Map<String, Object>>) flow.process(getTestEvent(workerRes.getEmployeeData().get(0).getEmployeeID(), 
				MessageExchangePattern.REQUEST_RESPONSE)).
										getMessage().getPayload();
		Map<String, Object> caseMap = iterator.next();
		SFDC_ID = caseMap.get("Id").toString();
		ACCOUNT_ID = caseMap.get("AccountId").toString();
		CONTACT_ID = caseMap.get("ContactId").toString();
		assertEquals("Subject should be synced", employee.getGivenName() + " " + employee.getFamilyName() + " Case", 
													caseMap.get("Subject"));
		assertEquals("Email should be synced", EMAIL, caseMap.get("SuppliedEmail"));
    }
    
    private void deleteTestDataFromSandBox() throws MuleException, Exception {
		// Delete the created users in SFDC
    	log.info("deleting test data...");
		SubflowInterceptingChainLifecycleWrapper deleteFlow = getSubFlow("deleteSFDC");
		deleteFlow.initialise();

		List<String> idList = new ArrayList<String>();
		idList.add(SFDC_ID);
		idList.add(ACCOUNT_ID);
		idList.add(CONTACT_ID);
		deleteFlow.process(getTestEvent(idList,
				MessageExchangePattern.REQUEST_RESPONSE));
		// Delete the created users in Workday
		SubflowInterceptingChainLifecycleWrapper flow = getSubFlow("getWorkdaytoTerminateFlow");
		flow.initialise();
		
		try {
			MuleEvent response = flow.process(getTestEvent(getEmployee(), MessageExchangePattern.REQUEST_RESPONSE));			
			flow = getSubFlow("terminateWorkdayEmployee");
			flow.initialise();
			flow.process(getTestEvent(prepareTerminate(response), MessageExchangePattern.REQUEST_RESPONSE));								
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
    
    private EmployeeGetType getEmployee(){
		EmployeeGetType get = new EmployeeGetType();
		EmployeeReferenceType empRef = new EmployeeReferenceType();					
		ExternalIntegrationIDReferenceDataType value = new ExternalIntegrationIDReferenceDataType();
		IDType idType = new IDType();
		value.setID(idType);
		idType.setSystemID("Salesforce - Chatter");
		idType.setValue(EXT_ID);			
		empRef.setIntegrationIDReference(value);
		get.setEmployeeReference(empRef);		
		return get;
	}
	
	private TerminateEmployeeRequestType prepareTerminate(MuleEvent response) throws DatatypeConfigurationException{
		TerminateEmployeeRequestType req = (TerminateEmployeeRequestType) response.getMessage().getPayload();
		TerminateEmployeeDataType eeData = req.getTerminateEmployeeData();		
		TerminateEventDataType event = new TerminateEventDataType();
		java.util.Calendar cal = java.util.Calendar.getInstance();
		cal.add(java.util.Calendar.DATE, 1);
		eeData.setTerminationDate(xmlDate(cal.getTime()));
		EventClassificationSubcategoryObjectType prim = new EventClassificationSubcategoryObjectType();
		List<EventClassificationSubcategoryObjectIDType> list = new ArrayList<EventClassificationSubcategoryObjectIDType>();
		EventClassificationSubcategoryObjectIDType id = new EventClassificationSubcategoryObjectIDType();
		id.setType("WID");
		id.setValue(TERMINATION_ID);
		list.add(id);
		prim.setID(list);
		event.setPrimaryReasonReference(prim);
		eeData.setTerminateEventData(event );
		return req;		
	}
	
	private static XMLGregorianCalendar xmlDate(Date date) throws DatatypeConfigurationException {
		GregorianCalendar gregorianCalendar = (GregorianCalendar) GregorianCalendar.getInstance();
		gregorianCalendar.setTime(date);
		return DatatypeFactory.newInstance().newXMLGregorianCalendar(gregorianCalendar);
	}
	
	
}
