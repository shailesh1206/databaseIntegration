package com.ae.intg.test;

import java.util.ArrayList;
import java.util.List;

import com.ae.intg.DBIntegrationController;
import com.automationedge.common.ext.integration.AeIntegrationTypeException;
import com.automationedge.common.ext.integration.IntgConfParameter;
import com.automationedge.common.ext.integration.IntgTypeConfiguration;

public class TestDBIC {
	static final String BASE_URL_KEY = "URL";
	static final String Username = "Username";
	static final String Password = "password";
	static final String Ports = "Ports";
	static final String DBName = "DBName";
	static final String TBName = "TBName";
	static final String Query = "Query";
	static final String FilePath = "Filepath";

	public static void main(String args[]) throws AeIntegrationTypeException {
		DBIntegrationController rc = new DBIntegrationController();
		IntgTypeConfiguration typeConf = new IntgTypeConfiguration();
		List<IntgConfParameter> configParams = new ArrayList<IntgConfParameter>();
		// IntgConfParameter url = new IntgConfParameter();
		// url.setName(BASE_URL_KEY);
		// url.setValue("https://vyom-dsom-rest.trybmc.com");
		// url.setValue("http://10.51.28.21:8008");
		// configParams.add(url);

		IntgConfParameter username = new IntgConfParameter();
		username.setName(Username);
		username.setValue("postgres");
		configParams.add(username);

		IntgConfParameter password = new IntgConfParameter();
		password.setName(Password);
		password.setValue("Shailesh@123");
		configParams.add(password);

		IntgConfParameter ports = new IntgConfParameter();
		ports.setName(Ports);
		ports.setValue("5432");
		configParams.add(ports);

		IntgConfParameter DBNAME = new IntgConfParameter();
		DBNAME.setName(DBName);
		DBNAME.setValue("Demo");
		configParams.add(DBNAME);

		IntgConfParameter tbname = new IntgConfParameter();
		tbname.setName(TBName);
		tbname.setValue("users");
		configParams.add(tbname);

		IntgConfParameter query = new IntgConfParameter();
		query.setName(Query);
		query.setValue("select * from users");
		configParams.add(query);

		IntgConfParameter filepath = new IntgConfParameter();
		filepath.setName(FilePath);
		filepath.setValue("E:\\IntegrationComponent\\Input\\Input.txt");
		configParams.add(filepath);

		System.out.println(configParams);
		typeConf.setConfigParams(configParams);

		rc.init(typeConf);
		// rc.populateConfParams(typeConf);
		rc.poll();

	}

}
