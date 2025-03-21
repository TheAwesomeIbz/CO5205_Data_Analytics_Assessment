import pandas as pd
from datetime import datetime


class ODSSchema:
    SaleFactTable = ""
    LocationDimension = ""
    CustomerDimension = ""
    CategoryDimension = ""
    OrderDimenstion = ""
    ProductDimension = ""

    def __init__(self) -> None:
        self.SaleFactTable = pd.DataFrame(columns=['SaleID', 'PostalCode', 'CustomerID', 'OrderID', 'SaleAmount', 'DateOfPurchase'])
        self.LocationDimension = pd.DataFrame(columns=['PostalCode', 'City', 'State', 'Country'])
        self.CustomerDimension = pd.DataFrame(columns=['CustomerID', 'FirstName', 'Surname', 'CustomerType'])
        self.CategoryDimension = pd.DataFrame(columns=['CategoryName', 'ParentCategory'])
        self.OrderDimenstion = pd.DataFrame(columns=['OrderID', 'ProductID', 'Quantity'])
        self.ProductDimension = pd.DataFrame(columns=['ProductID', 'ProductName', 'CategoryName', 'Subcategory', 'Cost', 'ProductPrice'])
        pass
    
    def dropAllDuplicates(self):
        self.CustomerDimension.drop_duplicates(subset=['CustomerID'], inplace=True)
        self.LocationDimension.drop_duplicates(subset=['PostalCode'], inplace=True)
        self.ProductDimension.drop_duplicates(subset=['ProductID'], inplace=True)
        self.OrderDimenstion.drop_duplicates(subset=['OrderID'], inplace=True)
        self.formatSaleFactTableDates()

        self.OrderDimenstion.sort_values(by=['OrderID'])

    

    def formatSaleFactTableDates(self):
        for index, row in self.SaleFactTable.iterrows():
            currentDate = row['DateOfSale']
            try:
                formattedDate = datetime.strptime(str(currentDate), "%d/%m/%Y").strftime("%Y-%m-%d")
                self.SaleFactTable.at[index, 'DateOfSale'] = formattedDate
            except Exception as error:
                continue
                
                

            
        
            
        