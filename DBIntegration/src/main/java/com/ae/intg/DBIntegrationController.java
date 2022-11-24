package com.ae.intg;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.automationedge.common.ext.integration.AeIntegrationTypeException;
import com.automationedge.common.ext.integration.AeIntgerationType;
import com.automationedge.common.ext.integration.IAeIntegrationType;
import com.automationedge.common.ext.integration.IntgConfParameter;
import com.automationedge.common.ext.integration.IntgTypeConfiguration;
import com.automationedge.model.AutomationRequest;
import com.automationedge.model.AutomationRequestStatusUpdate;
import com.automationedge.model.AutomationResponse;
import com.automationedge.util.JsonUtils;

@AeIntgerationType
public class DBIntegrationController implements IAeIntegrationType {
	private static final Logger LOGGER = LoggerFactory.getLogger(DBIntegrationController.class);
	private HashMap<String, IntgConfParameter> configurationParameters = new HashMap<>();
	private Connection c = null;
	private Statement stmt = null;

	public void init(IntgTypeConfiguration intgTypeConf) throws AeIntegrationTypeException {
		LOGGER.info("<DB> Entered init method");
		LOGGER.info("<DB> intgTypeConf " + intgTypeConf);

		// System.out.println("In init");
		DBConnection(intgTypeConf);
		LOGGER.debug("<DB> Exited init method");
	}

	public boolean testConnection(IntgTypeConfiguration intgTypeConf) throws AeIntegrationTypeException {
		LOGGER.debug("<DB> Entered testConnection method");
		LOGGER.debug("<DB> intgTypeConf " + intgTypeConf);
		try {
			DBConnection(intgTypeConf);

		} catch (AeIntegrationTypeException e) {
			LOGGER.error("<DB> Test connection failed ", e);
			return false;
		}
		LOGGER.debug("<DB> Exiting testConnection method");
		return true;
	}

	public void DBConnection(IntgTypeConfiguration intgTypeConf) throws AeIntegrationTypeException {
		LOGGER.debug("<DB> Entered Databaseconnection method");
		LOGGER.debug("<DB> intgTypeConf " + intgTypeConf);
		populateConfParams(intgTypeConf);
		try {
			String URL = "jdbc:postgresql://localhost:" + (configurationParameters.get(Constants.DB_Port).getValue())
					+ "/" + (configurationParameters.get(Constants.DB_DBNAME).getValue());
			// System.out.println(URL);
			Class.forName("org.postgresql.Driver");
			c = DriverManager.getConnection(URL, (configurationParameters.get(Constants.DB_USER_KEY).getValue()),
					(configurationParameters.get(Constants.DB_PASSWORD_KEY).getValue()));
		} catch (Exception e) {
			LOGGER.debug("<DB> Login failure, {}", e);
			throw new AeIntegrationTypeException(e);

		}
		LOGGER.debug("Opened database successfully");
		// System.out.println("Opened database successfully");
	}

	private void populateConfParams(IntgTypeConfiguration integrationTypeConf) throws AeIntegrationTypeException {
		LOGGER.debug("<DB> Entered populateConfParams method");
		if (integrationTypeConf.getConfigParams() == null) {
			throw new AeIntegrationTypeException("<DB> Integration Type configuration parameters is null");
		}

		LOGGER.debug("<DB> Configuration parameters received are: ");
		configurationParameters.clear();
		for (IntgConfParameter parameter : integrationTypeConf.getConfigParams()) {
			configurationParameters.put(parameter.getName(), parameter);
			LOGGER.debug("<DB> Key : {}", parameter.getName());

		}
		// System.out.println(configurationParameters);
		if ((configurationParameters.get(Constants.DB_USER_KEY) == null)
				|| (configurationParameters.get(Constants.DB_PASSWORD_KEY) == null)
				|| (configurationParameters.get(Constants.DB_Port) == null)
				|| (configurationParameters.get(Constants.DB_DBNAME) == null)
				|| (configurationParameters.get(Constants.DB_Query) == null)) {
			throw new AeIntegrationTypeException("<DB> The required configuration parameters are not present i.e."
					+ Constants.DB_USER_KEY + ", " + Constants.DB_PASSWORD_KEY + "," + Constants.DB_Port + ","
					+ Constants.DB_DBNAME + " and " + Constants.DB_Query);
		}
		LOGGER.debug("<DB> Exited populateConfParams method");
	}

	@Override
	public void cleanup() throws AeIntegrationTypeException {
		// TODO Auto-generated method stub

	}

	@Override
	public void downloadRequestAttachments(AutomationRequest arg0, File arg1) throws AeIntegrationTypeException {
		// TODO Auto-generated method stub

	}

	@Override
	public List<AutomationRequest> poll() throws AeIntegrationTypeException {
		LOGGER.debug("<DB> Entered poll method");
		LOGGER.info("<DB> Entered poll method");
		List<AutomationRequest> AERequestList = new ArrayList<>();
		boolean DBFlag;
		try {
			DBFlag = Query();
			if (DBFlag == true) {
				try {
					populateAutomationRequestList(AERequestList, DBFlag);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			LOGGER.debug("<DB> Query failure, {}", e1);
			throw new AeIntegrationTypeException(e1);

		}

		LOGGER.debug("<DB> Exiting poll method");
		// System.out.println("aeRequestList"+new Gson().toJson(aeRequestList));
		return AERequestList;
	}

	private boolean Query() throws AeIntegrationTypeException {
		// System.out.println(configurationParameters.get(Constants.AE_TBName).getValue());
		try {
			c.getMetaData();
			ResultSet res;
			stmt = c.createStatement();
			// res = meta.getTables(null, null,
			// (configurationParameters.get(Constants.DB_TBName).getValue()), new String[]
			// {"TABLE"});
			res = stmt.executeQuery((configurationParameters.get(Constants.DB_Query).getValue()));
			if (res.next()) {
				System.out.println(true);
				LOGGER.info("true");
				return true;
			} else {
				LOGGER.info("false");
				return false;
			}
		} catch (Exception e) {
			LOGGER.debug("<DB> Query failure, {}", e);
			throw new AeIntegrationTypeException(e);
		}

	}

	private void populateAutomationRequestList(List<AutomationRequest> aeRequestList, boolean dBFlag)
			throws Exception {
		LOGGER.debug("<DB> Entered populateAutomationRequestList method");
		System.out.println("<DB> Entered populateAutomationRequestList method");
		// AutomationRequest aeRequest;
		if (dBFlag == false) {
			LOGGER.info("<DB> No requests found");
			System.out.println("No requests found");
		} else {
			AutomationRequest aeRequest = new AutomationRequest();

			//String requestbody = "{\r\n    \"orgCode\": \"SHAILESH\",\r\n    \"workflowName\": \"EmailReadTest\",\r\n    \"source\": \"DB Integration Component\",\r\n    \"userId\": \"shailesh chaudhari\",\r\n    \"params\": [\r\n        {\r\n            \"name\": \"firstname\",\r\n            \"value\": \"Shailesh\",\r\n            \"type\": \"String\",\r\n            \"order\": 0,\r\n            \"secret\": false,\r\n            \"optional\": false,\r\n            \"displayName\": \"firstname\",\r\n            \"poolCredential\": false\r\n        },\r\n        {\r\n            \"name\": \"lastname\",\r\n            \"value\": \"Chaudhari\",\r\n            \"type\": \"String\",\r\n            \"order\": 0,\r\n            \"secret\": false,\r\n            \"optional\": false,\r\n            \"displayName\": \"lastname\",\r\n            \"poolCredential\": false\r\n        }\r\n    ]\r\n}";
			// Read AE workflow json File
			String requestbody = readFileAsString(configurationParameters.get(Constants.DB_FilePath).getValue());
			if (requestbody != null) {
				try {
					LOGGER.info("<DB> Workflow call");
					aeRequest = JsonUtils.deserialize(requestbody, AutomationRequest.class);
					aeRequestList.add(aeRequest);
					System.out.println("AE:" + aeRequest.getOrgCode());
				} catch (Exception e) {
					// TODO Auto-generated catch block
					LOGGER.info("<DB>Error while json deSerialization:" + e.getMessage());
					// System.out.println("Error while deserialization:"+e.getMessage());
				}

			}

			LOGGER.info("<DB> Requests response: AE Requests Count: {}", aeRequestList.size());
		}
		LOGGER.info("<DB> Exited populateAutomationRequestList method");
	}
	
	public static String readFileAsString(String fileName)throws Exception
	  {
	    String data = "";
	    data = new String(Files.readAllBytes(Paths.get(fileName)));
	    System.out.println(data);
	    return data;
	  }

	@Override
	public boolean updateResponse(AutomationResponse arg0, File arg1) throws AeIntegrationTypeException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public List<Long> updateStatus(List<AutomationRequestStatusUpdate> arg0) throws AeIntegrationTypeException {
		// TODO Auto-generated method stub
		return null;
	}

}
