use PizemData

--What is the most expensive product: 

SELECT ProductName AS Products, Price
FROM dbo.Products
ORDER BY Price DESC

--What is the average product price:

SELECT AVG (Price) 
FROM dbo.Products

--List of customers whose name start on B:

SELECT CustomerName 
FROM dbo.Customers
WHERE CustomerName LIKE 'b%'

--Number of products by category and supplier, sorted by the number of products:

SELECT CategoryName AS Category, SupplierName AS Suppliers, COUNT (DISTINCT ProductID) AS NumberOfProduct
FROM dbo.Products
JOIN dbo.Categories ON dbo.Products.CategoryID = dbo.Categories.CategoryID
JOIN dbo.Suppliers ON dbo.Products.SupplierID = dbo.Suppliers.SupplierID
GROUP BY CategoryName,SupplierName 
ORDER BY NumberOfProduct DESC

--List all customers who made more than one order:

SELECT CustomerName AS Customer, COUNT(DISTINCT OrderID) AS NumberOfOrders
FROM dbo.Customers
INNER JOIN dbo.Orders 
ON dbo.Customers.CustomerID = dbo.Orders.CustomerID
GROUP BY CustomerName
HAVING COUNT(DISTINCT OrderID) >1
ORDER BY NumberOfOrders DESC

--List customers who did not order anything:

SELECT CustomerName AS Customer, OrderID AS Orders
FROM dbo.Customers
LEFT JOIN dbo.Orders ON dbo.Customers.CustomerID = dbo.Orders.CustomerID
WHERE OrderID IS NULL
