package rs.etf.sab.student;

import rs.etf.sab.operations.*;
import rs.etf.sab.tests.TestHandler;
import rs.etf.sab.tests.TestRunner;

public class StudentMain {

	public static void main(String[] args) {

		ArticleOperations articleOperations = new vl190384_ArticleOperations();
		BuyerOperations buyerOperations = new vl190384_BuyerOperations();
		CityOperations cityOperations = new vl190384_CityOperations();
		GeneralOperations generalOperations = new vl190384_GeneralOperations();
		OrderOperations orderOperations = new vl190384_OrderOperations((vl190384_GeneralOperations) generalOperations);
		ShopOperations shopOperations = new vl190384_ShopOperations();
		TransactionOperations transactionOperations = new vl190384_TransactionOperations();

		TestHandler.createInstance(articleOperations, buyerOperations, cityOperations, generalOperations,
				orderOperations, shopOperations, transactionOperations);

		TestRunner.runTests();

	}
}
