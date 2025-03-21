import json
import urllib.parse
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from ODSSchema import ODSSchema
from sqlalchemy.sql import text

#7.	Build a Main class which executes all the files, so all the actions above are executed in a single action.  This main class should contain the links to all the database connections and CSV/JSON files.
class Sales:
    #PUBLIC
    odsSchema = None
    sqlSession = None

    #CONSTRUCTOR CLASS THAT CALLS ALL THE FUNCTIONS IN SEQUENTIAL ORDER
    #2.	Create an Operational Data Store (ODS) in a Python program that matches your star/snow-flake data warehouse schema in task 1.  
    def __init__(self) -> None:
        self.odsSchema = ODSSchema()
        
        if (self.__createSQLConnection()):
            self.obtainSQLDatabaseInformation()
            self.__initializeCSVData()
            self.__initializeJSONData()
            self.InitializeSaleIDsForDataFrame()
            self.odsSchema.dropAllDuplicates()
            print("DUPLICATE ITEMS DROPPED FROM OPERATIONAL DATA STORAGE!")
            self.__exportToSQLServer()
        else:
            print("No secure connection could be obtained.\nTerminating program...")
        
        
    #4.	SalesCSV contains a list of sales in a comma separate value format.  Create a Python class that will read in the data values from the CSV file and upload them to your operational data store.   		    
    def __initializeCSVData(self):
        print("Initializing CSV Data...")
        csvPath = "CO5205 - Assignment 2\BuildCSV.csv"
        csvDataframe = pd.read_csv(csvPath, encoding="ISO-8859-1")

        #ADD CSV FACT TABLE TO FACT TABLE ODS
        csvSaleFactTable = pd.concat([csvDataframe['Postal Code'], csvDataframe['Customer ID'], csvDataframe['Order ID'], csvDataframe['Sales'], csvDataframe['Order Date']], axis=1)
        csvSaleFactTable.rename(columns={"Postal Code" : "PostalCode", "Customer ID" : "CustomerID", "Order ID" : "OrderID", "Sales" : "SaleAmount", "Order Date" : "DateOfSale" }, inplace=True)
        csvSaleFactTable.insert(0, 'SaleID', "SALEID")
        
        for index, row in csvSaleFactTable.iterrows():
            self.odsSchema.SaleFactTable.loc[len(self.odsSchema.SaleFactTable)] = row
        
        #ADD CSV LOCATION INFORMATION TO ODS
        csvLocationDimension = pd.concat([csvDataframe['Postal Code'], csvDataframe['City'], csvDataframe['State'], csvDataframe['Country']], axis=1)
        csvLocationDimension.rename(columns={'Postal Code' : 'PostalCode'}, inplace=True)
        self.odsSchema.LocationDimension = pd.concat([self.odsSchema.LocationDimension, csvLocationDimension]).drop_duplicates()
        
        #ADD CSV CUSTOMER INFORMATION TO ODS
        csvNameDimension = pd.concat([csvDataframe['Customer ID'], csvDataframe['FirstName'], csvDataframe['Surname'], csvDataframe['Segment']], axis=1)
        csvNameDimension.rename(columns={"Customer ID" : "CustomerID", "Segment" : "CustomerType"}, inplace=True)
        for index, row in csvNameDimension.iterrows():
            self.odsSchema.CustomerDimension.loc[len(self.odsSchema.CustomerDimension)] = row

        #ADD ORDER INFORMATION TO ODS
        csvOrderDimension = pd.concat([csvDataframe['Order ID'], csvDataframe['Product ID'], csvDataframe['Quantity']], axis=1)
        csvOrderDimension.rename(columns={"Order ID" : "OrderID", "Product ID" : "ProductID"}, inplace=True)
        self.odsSchema.OrderDimenstion = pd.concat([self.odsSchema.OrderDimenstion, csvOrderDimension]).drop_duplicates()

        #ADD ITEM INFORMATION TO ODS
        csvProductInformation = pd.concat([csvDataframe['Product ID'], csvDataframe['Product Name'], csvDataframe['Category'], csvDataframe['Sub-Category']], axis=1)
        csvProductInformation.rename(columns={"Order ID" : "OrderID", "Product ID" : "ProductID",  "Product Name" : "ProductName", "Sub-Category" : "Subcategory"}, inplace=True)
        csvProductInformation.insert(len(csvProductInformation.columns), 'Cost', 0)
        csvProductInformation.insert(len(csvProductInformation.columns), 'ProductPrice', 0)

        for index, row in csvProductInformation.iterrows():
            csvProductInformation.loc[index, 'Cost'] = None
            csvProductInformation.loc[index, 'ProductPrice'] = None
            self.odsSchema.ProductDimension.loc[len(self.odsSchema.ProductDimension)] = row
            
        print("CSV DATA SUCCESSFULLY INITIALIZED!")
                
    #5.	SalesJSON contains a list of sales in a JSON format.  Add a class to your Python program that will read in the JSON data values and upload them to your operational data store.
    def __initializeJSONData(self):
         print("Initializing JSON data...")
         jsonPath = "CO5205 - Assignment 2\BuildJSON.json"
         with open(jsonPath, mode='r') as _jsonFile:
            data = json.load(_jsonFile)
            self.__jsonDataframe = pd.json_normalize(data['Sales'])
            #This code explodes the array in the items column
            #from there, the items column is removed, and in place it is replaced with the values inside the object
            #axis essentially means to replace the new columns on the existing column, in this case items
            jsonDataframe = self.__jsonDataframe.explode('Items')
            jsonDataframe = pd.concat(
                [
                    jsonDataframe.drop(['Items'], axis=1),
                    jsonDataframe['Items'].apply(pd.Series)
                ], axis=1
            )

            #ADD JSON FACT TABLE TO FACT TABLE ODS
            jsonFactTable = pd.concat([jsonDataframe['Postal Code'], jsonDataframe['Customer ID'], jsonDataframe['Order ID'], jsonDataframe['Sales'], jsonDataframe['Order Date']], axis=1)
            jsonFactTable.rename(columns={"Postal Code" : "PostalCode", "Customer ID" : "CustomerID", "Order ID" : "OrderID", "Sales" : "SaleAmount", "Order Date" : "DateOfSale" }, inplace=True)
            jsonFactTable.insert(0, 'SaleID', "SALEID")

            for index, row in jsonFactTable.iterrows():
                self.odsSchema.SaleFactTable.loc[len(self.odsSchema.SaleFactTable)] = row
            
            #ADD JSON LOCATION INFORMATION TO ODS
            jsonLocationDimension = pd.concat([jsonDataframe['Postal Code'], jsonDataframe['City'], jsonDataframe['State'], jsonDataframe['Country']], axis=1).drop_duplicates()
            jsonLocationDimension.rename(columns={'Postal Code' : 'PostalCode'}, inplace=True)
            self.odsSchema.LocationDimension = pd.concat([self.odsSchema.LocationDimension, jsonLocationDimension]).drop_duplicates()

            #ADD JSOM INFORMATION TO ODS
            jsonOrderDimension = pd.concat([jsonDataframe['Order ID'], jsonDataframe['Product ID'], jsonDataframe['Quantity']], axis=1)
            jsonOrderDimension.rename(columns={"Order ID" : "OrderID", "Product ID" : "ProductID"}, inplace=True)
            self.odsSchema.OrderDimenstion = pd.concat([self.odsSchema.OrderDimenstion, jsonOrderDimension]).drop_duplicates()

            print("JSON DATA SUCCESSFULLY INITIALIZED!")

    #Creates a connection to the SQL Server, which is a private function called on class instantiation
    def __createSQLConnection(self) -> bool:
        try:
            print("Creating connection to SQL Server...")
            connection = '''DRIVER={SQL Server};
                    SERVER=mssql.chester.network;
                    DATABASE=db_2113691_2113691_co5205;
                    UID=user_db_2113691_2113691_co5205;
                    PWD=P@55word'''
            params = urllib.parse.quote_plus(connection)
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            Session = sessionmaker(bind=engine)
            self.sqlSession = Session()
            print("Successful Connection Established!")
            return True
        except:
            print("An error has occured!")
            return False
    #3a. Execute this script and load all the tables and data into a database on the Chester.Network using Microsoft’s SQL Server Management Studio or equivalent.
    #3b Write a Python class that will extract all data from the SQL Server database generated by Script.sql into your operational data store, which you created in task 2.   
    def obtainSQLDatabaseInformation(self):
        salesTable = self.obtainInformationFromQuery("Select * from sale")
        saleItemTable = self.obtainInformationFromQuery("Select * from saleitem")
        productTable = self.obtainInformationFromQuery("Select * from Product")
        customerTable = self.obtainInformationFromQuery("Select * from Customer")
        categoryTable = self.obtainInformationFromQuery("Select * from Category")
        
        # self.productCategoryDictionary = dict(zip(productTable['ProductID'], productTable['Category']))
        #ADD ALL FIELDS INTO FACT TABLE
        self.odsSchema.SaleFactTable = pd.concat([salesTable['PostalCode'], salesTable['CustomerID'], salesTable['OrderID'], salesTable['SaleAmount'], salesTable['DateOfSale']], axis=1)
        self.odsSchema.SaleFactTable.insert(0, 'SaleID', "SALEID")

        #ADD THE REST OF THE FIELDS
        self.odsSchema.LocationDimension = pd.concat([salesTable['PostalCode'], salesTable['City'], salesTable['State'], salesTable['Country']], axis=1).sort_values(by=['PostalCode'])
        self.odsSchema.CategoryDimension = categoryTable
        self.odsSchema.CustomerDimension = customerTable
        self.odsSchema.OrderDimenstion = saleItemTable
        self.odsSchema.ProductDimension = productTable
        self.productDictionary = dict(zip(productTable['ProductID'], list(zip(productTable['Cost'], productTable['ProductPrice']))))
      
    #CREATE UNIQUE VALUES FOR EACH SALE IN THE SALES FACT TABLE
    def InitializeSaleIDsForDataFrame(self):
        for index, row in self.odsSchema.SaleFactTable.iterrows():
            self.odsSchema.SaleFactTable.loc[index, 'SaleID'] = "SALE" + "{:04d}".format(index)
        self.odsSchema.dropAllDuplicates()

    #FUNCTION USED TO OBTAIN INFORMATION FROM SQL QUERIES
    def obtainInformationFromQuery(self, query : str) -> pd.DataFrame:
        try:
            executedQuery = pd.read_sql_query(query, self.sqlSession.bind)
            return executedQuery
        except:
            print("The query could not be executed properly!")
            return pd.DataFrame.empty

    #FUNCTION USED TO EXPORT ALL FILES AND DATA OBTAINED WITHIN THE ODS SCHEMA
    def __exportToSQLServer(self):
        print("Exporting to SQL Server...")
        try:
            connection = '''DRIVER={SQL Server};
                    SERVER=mssql.chester.network;
                    DATABASE=db_2113691_2113691_co5205_ods;
                    UID=user_db_2113691_2113691_co5205_ods;
                    PWD=P@55word'''

            params = urllib.parse.quote_plus(connection)
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            Session = sessionmaker(bind=engine)
            session = Session()

            self.__executeDatabaseInitialisation(session)
            self.__executeSQLCommands(session)
            print("SUCCESSFULLY COMMITTED TO SQL SERVER!")
        except Exception as error:
            print(f"An error has occured!\n Error = {error}")
            return

    #CALLED TO INITIALIZE SQL SERVER TABLES AND TABLE KEYS
    #6.	Create an empty SQL Server database on the Chester.Network and export all the data in your operational data store into this database.
    def __executeDatabaseInitialisation(self, session):
        print("Initializing SQL Server Tables...")
        session.execute(text('''
DROP TABLE IF EXISTS tbl_SaleFactTable
DROP TABLE IF EXISTS tbl_OrderDimension
DROP TABLE IF EXISTS tbl_LocationDimension
DROP TABLE IF EXISTS tbl_CustomerDimension
DROP TABLE IF EXISTS tbl_ProductDimension
DROP TABLE IF EXISTS tbl_CategoryDimension

CREATE TABLE tbl_CategoryDimension(
    CategoryName nvarchar(15) PRIMARY KEY,
    ParentCategory nvarchar(15),
)

CREATE TABLE tbl_ProductDimension(
    ProductID nvarchar(150) PRIMARY KEY,
    ProductName nvarchar(500),
    Category nvarchar(15),
    Subcategory nvarchar(15),
    Cost money,
    ProductPrice money,
    FOREIGN KEY (Category) REFERENCES tbl_CategoryDimension(CategoryName)
)

CREATE TABLE tbl_CustomerDimension(
    CustomerID nvarchar(250) PRIMARY KEY,
    FirstName nvarchar(250),
    Surname nvarchar(250),
    CustomerType nvarchar(250)
)

CREATE TABLE tbl_LocationDimension(
	PostalCode int NOT NULL PRIMARY KEY,
	City varchar(50),
	State varchar(50),
	Country varchar(50),
)

CREATE TABLE tbl_OrderDimension(
    OrderID nvarchar(150) NOT NULL PRIMARY KEY,
	ProductID nvarchar(150),
    Quantity int,
    FOREIGN KEY (ProductID) REFERENCES tbl_ProductDimension(ProductID)
)

CREATE TABLE tbl_SaleFactTable(
	SaleID varchar(10) NOT NULL PRIMARY KEY,
	PostalCode int,
    OrderID nvarchar(150),
    DateOfSale date,
    SaleAmount money,
    CustomerID nvarchar(250),
                             
    FOREIGN KEY (OrderID) REFERENCES tbl_OrderDimension(OrderID),
    FOREIGN KEY (PostalCode) REFERENCES tbl_LocationDimension(PostalCode),
    FOREIGN KEY (CustomerID) REFERENCES tbl_CustomerDimension(CustomerID),
)
'''))

    #CALLED TO EXECUTE ALL SQL COMMANDS
    def __executeSQLCommands(self, session):
        try:
            for index, row in self.odsSchema.CustomerDimension.iterrows():
                customerID = f'''{row['CustomerID']}'''.replace("'", "")
                firstName = f'''{row['FirstName']}'''.replace("'", "")
                surname= f'''{row['Surname']}'''.replace("'", "")
                customerType = f'''{row['CustomerType']}'''.replace("'", "")
                session.execute(text(f"INSERT INTO tbl_CustomerDimension (CustomerID, FirstName, Surname, CustomerType) VALUES ('{customerID}', '{firstName}', '{surname}', '{customerType}')"))
            session.commit()
            print("Customer dimension Added.")

            for index, row in self.odsSchema.CategoryDimension.iterrows():
                session.execute(text(f"INSERT INTO tbl_CategoryDimension (CategoryName, ParentCategory) VALUES ('{row['CategoryName']}', '{row['ParentCategory']}')"))
            session.commit()
            print("Category dimension Added.")

            for index, row in self.odsSchema.LocationDimension.iterrows():
                session.execute(text(f"INSERT INTO tbl_LocationDimension (PostalCode, City, State, Country) VALUES ('{row['PostalCode']}', '{row['City']}', '{row['State']}', '{row['Country']}')"))
            session.commit()
            print("Location dimension Added.")

            for index, row in self.odsSchema.ProductDimension.iterrows():
                productName = f'''{row['ProductName']}'''.replace("'", "")
                session.execute(text(f"INSERT INTO tbl_ProductDimension (ProductID,ProductName,Category,Subcategory,Cost,ProductPrice) VALUES ('{row['ProductID']}', '{productName}', '{row['Category']}','{row['Subcategory']}','{row['Cost']}','{row['ProductPrice']}')"))
            session.commit()
            print("Product dimension Added.")

            for index, row in self.odsSchema.OrderDimenstion.iterrows():
                session.execute(text(f"INSERT INTO tbl_OrderDimension (ProductID, OrderID, Quantity) VALUES ('{row['ProductID']}', '{row['OrderID']}', '{row['Quantity']}')"))
            session.commit()
            print("Order dimension Added.")

            for index, row in self.odsSchema.SaleFactTable.iterrows():
                session.execute(text(f"INSERT INTO tbl_SaleFactTable (SaleID, PostalCode, OrderID, DateOfSale, SaleAmount, CustomerID) VALUES ('{row['SaleID']}', '{row['PostalCode']}', '{row['OrderID']}', '{row['DateOfSale']}', '{row['SaleAmount']}', '{row['CustomerID']}')"))
            session.commit()
            print("Sale Fact Table Added.")
            

            

        except Exception as error:
            print(f"an error has occured {error}")
            return
                    
sales = Sales()