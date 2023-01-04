use datacosupplychain;

#I. Create table

create table if not exists Co_Supply(
    Type varchar(200),
    Days_for_shipment_reality tinyint,
    Days_for_shipment_scheduled tinyint,
    Benefit_per_order decimal(5,2),
    Sales_per_customer decimal(5,2),
    Delivery_Status varchar(200),
    Late_delivery_risk tinyint,
    Category_Id int,
    Category_Name varchar(200),
    Customer_Country varchar(200),
    Customer_Id int,
    Customer_Segment varchar(200),
    Customer_State varchar(2),
    Customer_Zipcode int,
    Department_Id TINYINT,
    Department_Name varchar(50),
    Market varchar(50),
    Order_Country varchar(50),
    order_date_DateOrders Date,
    Order_Id int,
    Order_Item_Cardprod_Id int,
    Order_Item_Discount decimal(5,2),
    Order_Item_Discount_Rate decimal(3,2),
    Order_Item_Id int,
    Order_Item_Product_Price decimal(6,2),
    Order_Item_Profit_Ratio decimal(3,2),
    Order_Item_Quantity TINYINT,
    Sales decimal(6,2),
    Order_Item_Total decimal(6,2),
    Order_Profit_Per decimal(6,2),
    Order_Region varchar(100),
    Order_State varchar(100),
    Order_Status varchar(100),
    Order_Zipcode int,
    Product_Card_Id int,
    Product_Category_Id int,
    Product_Name varchar(100),
    Product_Price decimal(6,2),
    Product_Status tinyint,
    shipping_date_DateOrders date,
    Shipping_Mode varchar(100)); #Create table

SET GLOBAL local_infile=1;

SHOW GLOBAL VARIABLES LIKE 'local_infile';

LOAD DATA LOCAL INFILE 'F:/Working/DA/Project/Supply chain/DataCoSupplyChainDataset_off.csv'
INTO TABLE  Co_Supply
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS; #Import data into table

#II.Data cleaning

#1. Check data

select * from co_supply; # Check table

select count(*) from co_supply; # Check number of rows 

#2 Duplicated rows

select Order_Id, Sales_per_customer, Product_Card_Id, Benefit_per_order,Order_Item_Discount,count(*)
from co_supply
group by Order_Id, Sales_per_customer, Product_Card_Id, Benefit_per_order,Order_Item_Discount
order by count(*) desc; # Check if there are any duplicate rows
#=> no duplicate rows

#3 Null Values

select * from co_supply;

## Check the null values in all the columns of the table

select * from co_supply
where Type ="";

select * from co_supply
where Benefit_per_order ='';

select * from co_supply
where Delivery_Status ='';

select * from co_supply
where Category_Id ='';

select * from co_supply
where Category_Name ='';

select * from co_supply
where Customer_Country ='';

select * from co_supply
where Customer_Id='';

select * from co_supply
where Customer_Segment='';

select * from co_supply
where Customer_State='';

select * from co_supply
where Customer_Zipcode='';

select * from co_supply
where Department_Id='';

select * from co_supply
where Department_Name='';

select * from co_supply
where Market='';

select * from co_supply
where Order_Country ='';

select * from co_supply
where order_date_DateOrders is null;

select * from co_supply
where Order_Id ='';

select * from co_supply
where Order_Item_Cardprod_Id ='';

select * from co_supply
where Order_Item_Discount =''
and Sales < Order_Item_Total;

select * from co_supply
where Order_Item_Discount_Rate =''
and Sales != Order_Item_Total;

select * from co_supply
where Order_Item_Id ='';

select * from co_supply
where Order_Item_Product_Price ='';

select * from co_supply
where Order_Item_Profit_Ratio =''
and Benefit_per_order > 0;

select * from co_supply
where Order_Item_Quantity ='';

select * from co_supply
where Sales ='';

select * from co_supply
where Order_Item_Total ='';

select * from co_supply
where Order_Profit_Per_Order=''
and Benefit_per_order > 0;

select * from co_supply
where Order_Region='';

select * from co_supply
where Order_State='';
#=> There are some orders which are from Order Region West Asia do not have Order State 

select Order_State, count(distinct Order_Id) from co_supply
where Order_Region='West Asia'
group by Order_State
order by count(*) desc; #Identify the most count Order Id from Order State of The Order Region West Asia
#=> Order State: Estambul

update co_supply
set Order_State = 'Estambul'
where Order_State =''; #assign Order State value Estambul for the rows that have null values in Order State 

select * from co_supply
where Order_Status='';

select distinct Order_Status, count(distinct order_id) from co_supply
group by Order_Status
order by count(distinct order_id) desc;

select * from co_supply;

update co_supply
set Order_Status = 'COMPLETE'
where Delivery_Status != 'Shipping canceled';

select * from co_supply
where Product_Card_Id='';

select * from co_supply
where Product_Name='';

select * from co_supply
where Product_Price='';

select * from co_supply
where Product_Stock_Status='';

ALTER TABLE co_supply RENAME COLUMN Product_Status TO Product_Stock_Status; 

select * from co_supply
where shipping_date_DateOrders is null;

select * from co_supply
where Shipping_Mode = '';

select distinct Shipping_Mode, count(distinct order_id) from co_supply
group by Shipping_Mode
order by count(distinct order_id) desc;

select distinct Shipping_Mode, count(distinct order_id) from co_supply
group by Shipping_Mode
order by count(distinct order_id) desc;

select avg(datediff(shipping_date_DateOrders,order_date_DateOrders)), Shipping_Mode
from co_supply
group by Shipping_Mode;

#III.Data Analyst

select * from co_supply;

select min(order_date_DateOrders), max(order_date_DateOrders)
from co_supply; #Identify the period of time
#=> From 2015-01-01 to 2018-01-31

ALTER table co_supply
add column Order_year varchar(4); #add column order year

update co_supply
set Order_year = left(order_date_DateOrders,4);

#1 The Revenue, Benefit and number of orders through Year?

select Order_year,sum(Sales_per_customer) as 'Revenue' ,sum(Benefit_per_order) as 'Benefit', count(distinct Order_Id) as 'Number of orders'
from co_supply
where Order_Status = 'COMPLETE'
group by Order_year
order by Order_year; #Calculate the Revenue,Benefit, number of order of Each year
#=> the revenue,Benefit and number of orders of 2015,2016 are quite balanced. 
# the revenue,Benefit and number of orders of 2017 has increased but not significantly

#2 The Revenue, Benefit and number of orders By Order_Region?

select Order_Region,sum(Sales_per_customer) as 'Revenue' ,sum(Benefit_per_order) as 'Benefit', count(distinct Order_Id) as 'Number of orders'
from co_supply
where Order_Status = 'COMPLETE'
group by Order_Region
order by count(distinct Order_Id) desc;
#=> Western Europe, Central America and South America are the top 3 in Number of Orders and Benfit

select Order_Region,sum(Sales_per_customer) as 'Revenue' ,sum(Benefit_per_order) as 'Benefit', count(distinct Order_Id) as 'Number of orders'
from co_supply
where Order_Status = 'COMPLETE'
group by Order_Region
order by count(distinct Order_Id) desc;

select * from co_supply;

select Order_Region,sum(Sales_per_customer) as 'Revenue' ,sum(Benefit_per_order) as 'Benefit', count(distinct Order_Id) as 'Number of orders'
from co_supply
where Order_Status = 'COMPLETE'
and Order_year = '2017'
group by Order_Region
order by count(distinct Order_Id) desc;
# In 2017, Western Europe, Central America and South America are the top 3 in Number of Orders and Benfit

select Order_Region,sum(Sales_per_customer) as 'Revenue' ,sum(Benefit_per_order) as 'Benefit', count(distinct Order_Id) as 'Number of orders'
from co_supply
where Order_Status = 'COMPLETE'
and Order_year = '2016'
group by Order_Region
order by count(distinct Order_Id) desc;
# In 2016, West of USA, East of USA and US Center are the top 3 in Number of Orders and Benfit

select Order_Region,sum(Sales_per_customer) as 'Revenue' ,sum(Benefit_per_order) as 'Benefit', count(distinct Order_Id) as 'Number of orders'
from co_supply
where Order_Status = 'COMPLETE'
and Order_year = '2015'
group by Order_Region
order by sum(Benefit_per_order) desc;
# In 2015, Western Europe, Central America and South America are the top 3 in Number of Orders and Benfit

#3 The Revenue, Benefit and number of orders By Order_Country:

select Order_Country,sum(Sales_per_customer) as 'Revenue' ,sum(Benefit_per_order) as 'Benefit', count(distinct Order_Id) as 'Number of orders',
(count(distinct Order_Id) / ( SELECT count(distinct Order_Id) FROM co_supply where Order_Status = 'COMPLETE') * 100 ) AS  '% of orders'
from co_supply
where Order_Status = 'COMPLETE'
group by Order_Country
order by count(distinct Order_Id) desc;  # Calculate The Revenue, Benefit and number of orders By Order Country
# USA, France, Mexico,Australia, Alemania are top 5 countries with the highest number of orders

select count(distinct Order_Region)
from co_supply;

select count(distinct Order_Country)
from co_supply;


select Order_Country,sum(Sales_per_customer) as 'Revenue' ,sum(Benefit_per_order) as 'Benefit', count(distinct Order_Id) as 'Number of orders',
(count(distinct Order_Id) / ( SELECT count(distinct Order_Id) FROM co_supply where Order_Status = 'COMPLETE' and Order_year = '2017') * 100 ) AS  '% of orders'
from co_supply
where Order_Status = 'COMPLETE' and Order_year = '2017'
group by Order_Country
order by count(distinct Order_Id) desc;
#In 2017, France, Mexico,Alemania ,United Kingdom, Brazil are top 5 countries with the highest number of orders

select Order_Country,sum(Sales_per_customer) as 'Revenue' ,sum(Benefit_per_order) as 'Benefit', count(distinct Order_Id) as 'Number of orders',
(count(distinct Order_Id) / ( SELECT count(distinct Order_Id) FROM co_supply where Order_Status = 'COMPLETE' and Order_year = '2016') * 100 ) AS  '% of orders'
from co_supply
where Order_Status = 'COMPLETE' and Order_year = '2016'
group by Order_Country
order by count(distinct Order_Id) desc;
#In 2016, USA, Australia,Turkey ,China, India are top 5 countries with the highest number of orders

select Order_Country,sum(Sales_per_customer) as 'Revenue' ,sum(Benefit_per_order) as 'Benefit', count(distinct Order_Id) as 'Number of orders',
(count(distinct Order_Id) / ( SELECT count(distinct Order_Id) FROM co_supply where Order_Status = 'COMPLETE' and Order_year = '2015') * 100 ) AS  '% of orders'
from co_supply
where Order_Status = 'COMPLETE' and Order_year = '2015'
group by Order_Country
order by count(distinct Order_Id) desc;
#In 2015, France, Mexico,Alemania ,United Kingdom, Brazil are top 5 countries with the highest number of orders

#4 The Revenue, Benefit and number of orders By Customer_segment?

select Customer_Segment,sum(Sales_per_customer) as 'Revenue' ,sum(Benefit_per_order) as 'Benefit', count(distinct Order_Id) AS 'Number of orders', 
(count(distinct Order_Id) / ( SELECT count(distinct Order_Id) FROM co_supply where Order_Status = 'COMPLETE') * 100 ) AS  '% of orders'
from co_supply
where Order_Status = 'COMPLETE'
group by Customer_Segment
order by count(distinct Order_Id) desc; # Calculate The Revenue, Benefit and number of orders By Customer_segment
# Major of orders are ordered by Individual Customer with more than 50% orders, 30% orders are from Corporate and 18% Orders are from Home office

#5 The Revenue, Benefit and number of orders By Categories names?

select distinct Category_Name from co_supply;  

select Category_Name,sum(Sales_per_customer) as 'Revenue' ,sum(Benefit_per_order) as 'Benefit', count(distinct Order_Id) AS 'Number of orders', 
(count(distinct Order_Id) / ( SELECT count(distinct Order_Id) FROM co_supply where Order_Status = 'COMPLETE') * 100 ) AS  '% of orders'
from co_supply
where Order_Status = 'COMPLETE'
group by Category_Name
order by count(distinct Order_Id) desc; # Calculate The Revenue, Benefit and number of orders By Category Name
#=> Cleats, Men's footweare, Women's Apparel, Indoor/Outdoor Games, Fishing are the 5 items with the highest number of order

#6 Top Product Name:
select Product_Name,Category_Name,sum(Sales_per_customer) as 'Revenue' ,sum(Benefit_per_order) as 'Benefit',count(distinct Order_Id) AS 'Number of orders', 
(count(distinct Order_Id) / ( SELECT count(distinct Order_Id) FROM co_supply where Order_Status = 'COMPLETE') * 100 ) AS  '% of orders'
from co_supply
where Order_Status = 'COMPLETE'
group by Product_Name
order by count(distinct Order_Id) desc; #Calculate the top Products with the highest number of orders

select Product_Name, Category_Name,sum(Order_Item_Quantity) as 'Quantity Items'
from co_supply
where Order_Status = 'COMPLETE' 
group by Product_Name
order by sum(Order_Item_Quantity) desc;

select Product_Name, Category_Name,sum(Order_Item_Quantity) as 'Quantity Items'
from co_supply
where Order_Status = 'COMPLETE' 
and Order_Year = '2017'
group by Product_Name
order by sum(Order_Item_Quantity) desc;

select Product_Name, Category_Name,sum(Order_Item_Quantity) as 'Quantity Items'
from co_supply
where Order_Status = 'COMPLETE' 
and Order_Year = '2016'
group by Product_Name
order by sum(Order_Item_Quantity) desc;

select Product_Name, Category_Name,sum(Order_Item_Quantity) as 'Quantity Items'
from co_supply
where Order_Status = 'COMPLETE' 
and Order_Year = '2015'
group by Product_Name
order by sum(Order_Item_Quantity) desc;

#7 Top categories of Order Region that have the high number of Orders:

#a Western Europe
select Category_Name,sum(Sales_per_customer) as 'Revenue' ,sum(Benefit_per_order) as 'Benefit', count(distinct Order_Id) AS 'Number of orders', 
(count(distinct Order_Id) / ( SELECT count(distinct Order_Id) FROM co_supply where Order_Status = 'COMPLETE') * 100 ) AS  '% of orders'
from co_supply
where Order_Status = 'COMPLETE' and Order_Region = 'Western Europe'
group by Category_Name
order by count(distinct Order_Id) desc; # Calculate The Revenue, Benefit and number of orders By Category Name of Western Europe Region
#=> Also Cleats, Men's footweare, Women's Apparel, Indoor/Outdoor Games, Fishing

#b Market LATAM: include Region Central America and South America
select Category_Name,sum(Sales_per_customer) as 'Revenue' ,sum(Benefit_per_order) as 'Benefit', count(distinct Order_Id) AS 'Number of orders', 
(count(distinct Order_Id) / ( SELECT count(distinct Order_Id) FROM co_supply where Order_Status = 'COMPLETE') * 100 ) AS  '% of orders'
from co_supply
where Order_Status = 'COMPLETE' and Market = 'LATAM'
group by Category_Name
order by count(distinct Order_Id) desc; 
#=> Also Cleats, Men's footweare, Women's Apparel, Indoor/Outdoor Games, Fishing

#8 Number of Orders by Delivery Status:

select Delivery_Status, count(distinct Order_Id) AS 'Number of orders', 
(count(distinct Order_Id) / ( SELECT count(distinct Order_Id) FROM co_supply where Order_Status = 'COMPLETE') * 100 ) AS  '% of orders'
from co_supply
where Order_Status = 'COMPLETE'
group by Delivery_Status
order by count(distinct Order_Id) desc; #Calculate the number of Orders by delivery Status
#=> About 57% Orders are delivered late

select Delivery_Status, count(distinct Order_Id) AS 'Number of orders', 
(count(distinct Order_Id) / ( SELECT count(distinct Order_Id) FROM co_supply where Order_Status = 'COMPLETE' and Order_year = '2017') * 100 ) AS  '% of orders'
from co_supply
where Order_Status = 'COMPLETE' and Order_year = '2017'
group by Delivery_Status
order by count(distinct Order_Id) desc;
#=> In 2017, also About 57% Orders are delivered late. 


# Calculate late delivery rate of each shipping mode

select Shipping_Mode, count(distinct Order_Id) AS 'Number of orders', 
(count(distinct Order_Id) / ( SELECT count(distinct Order_Id) FROM co_supply where Shipping_Mode = 'First Class') * 100 ) AS  '% of orders'
from co_supply
where Delivery_status = 'Late delivery'
and Shipping_Mode = 'First Class'
order by count(distinct Order_Id) desc;
#=>95%

select Shipping_Mode, count(distinct Order_Id) AS 'Number of orders', 
(count(distinct Order_Id) / ( SELECT count(distinct Order_Id) FROM co_supply where Shipping_Mode = 'Second Class') * 100 ) AS  '% of orders'
from co_supply
where Delivery_status = 'Late delivery'
and Shipping_Mode = 'Second Class'
order by count(distinct Order_Id) desc;
#=> 76%

select Shipping_Mode, count(distinct Order_Id) AS 'Number of orders', 
(count(distinct Order_Id) / ( SELECT count(distinct Order_Id) FROM co_supply where Shipping_Mode = 'Standard Class') * 100 ) AS  '% of orders'
from co_supply
where Delivery_status = 'Late delivery'
and Shipping_Mode = 'Standard Class'
order by count(distinct Order_Id) desc;
#=> 38%

select Shipping_Mode, count(distinct Order_Id) AS 'Number of orders', 
(count(distinct Order_Id) / ( SELECT count(distinct Order_Id) FROM co_supply where Shipping_Mode = 'Same Day') * 100 ) AS  '% of orders'
from co_supply
where Delivery_status = 'Late delivery'
and Shipping_Mode = 'Same Day'
order by count(distinct Order_Id) desc;
#=> 46%

#=> The late delivery rate of all delivery methods is quite high, especially the two delivery methods, 
# First class with 95% of orders delivered late, and Second Class with 76% of orders delivered late

#9 Identify the ORder Region that have the most orders that are delivered late:

select Order_Region, count(distinct Order_Id) AS 'Number of orders', 
(count(distinct Order_Id) / ( SELECT count(distinct Order_Id) FROM co_supply where Order_Status = 'COMPLETE' and Delivery_Status = 'Late delivery') * 100 ) AS  '% of orders'
from co_supply
where Delivery_Status = 'Late delivery' and Order_Status = 'COMPLETE'
group by Order_Region
order by count(distinct Order_Id) desc;

select avg(Days_for_shipment_reality - Days_for_shipment_scheduled)
from co_supply
where Delivery_Status = 'Late delivery'  and Order_Status = 'COMPLETE';
#=> Avg: 1.6 days

#10 Top customer by the number of Orders:

select Customer_Id, Order_Year,Order_Country, sum(Sales_per_customer) as 'Revenue' ,sum(Benefit_per_order) as 'Benefit',count(distinct Order_Id) AS 'Number of orders'
from co_supply
where Order_Status = 'COMPLETE'
group by Customer_Id, Order_Year
order by count(distinct Order_Id) desc; #Calculate the number of Orders by the customer

select Order_year,count(distinct Customer_Id)
from co_supply
where Order_Status = 'COMPLETE' 
group by Order_year; #Calculate the number of customers each year

select Order_year,sum(sales_per_customer),count(distinct customer_id), sum(sales_per_customer)/count(distinct customer_id)
from co_supply
where Order_Status = 'COMPLETE'
and Order_year != 2018
group by Order_year;

#11 Identify Return Customer by Year:

select count(distinct customer_id) as 'Return Customer 2016',(count(distinct customer_id)/(select count(distinct customer_id) from co_supply where Order_Status = 'COMPLETE' and Order_Year = '2015')) as '% Return Customer 2016'
from co_supply
where order_year = '2016'
and Order_Status = 'COMPLETE'
and customer_id in (select customer_id from co_supply where order_year = 2015 and Order_Status = 'COMPLETE');
#=> 80% Customers of 2015 have returned to purchase in 2016

select count(distinct customer_id) as 'Return Customer 2017',(count(distinct customer_id)/( select count(distinct customer_id) from co_supply where Order_Status = 'COMPLETE' and Order_Year = '2016')) as 'Return Customer 2017'
from co_supply
where order_year = '2017'
and Order_Status = 'COMPLETE'
and customer_id in (select customer_id from co_supply where order_year = 2016 and Order_Status = 'COMPLETE');
#=> 70% Customers of 2016 have returned to purchase in 2017

select * from co_supply;

select Delivery_Status, count(Benefit_per_order)
from co_supply
where Benefit_per_order < 0
group by Delivery_Status
order by count(Benefit_per_order) desc;

select Delivery_Status, count(Benefit_per_order)
from co_supply
group by Delivery_Status
order by count(Benefit_per_order) desc;

select Customer_Id,Order_year,count(distinct Order_id) 
from co_supply
where Delivery_Status = 'Late delivery'
group by Customer_Id, Order_year
order by Customer_Id, count(distinct Order_id) desc;

select * from co_supply;

select count(distinct customer_id) as 'Return Customer 2016',(count(distinct customer_id)/(select count(distinct customer_id) from co_supply where Order_Status = 'COMPLETE' and Order_Year = '2015')) as '% Return Customer 2016'
from co_supply
where order_year = '2016'
and Order_Status = 'COMPLETE'
and customer_id in (select customer_id from co_supply where order_year = 2015 and Order_Status = 'COMPLETE');

select count(distinct customer_id) as 'New Customer 2016',(count(distinct customer_id)/(select count(distinct customer_id) from co_supply where Order_Status = 'COMPLETE' and Order_Year = '2015')) as '% Return Customer 2016'
from co_supply
where order_year = '2016'
and Order_Status = 'COMPLETE'
and customer_id not in (select customer_id from co_supply where order_year = 2015 and Order_Status = 'COMPLETE');

select count(distinct customer_id) as 'All Customer 2016',(count(distinct customer_id)/(select count(distinct customer_id) from co_supply where Order_Status = 'COMPLETE' and Order_Year = '2015')) as '% Return Customer 2016'
from co_supply
where order_year = '2016'
and Order_Status = 'COMPLETE';

select Delivery_Status, Count(distinct Customer_Id)
from co_supply
where Order_year = '2015'
and Order_Status = 'COMPLETE'
and customer_id not in (select customer_id from co_supply where order_year = 2016 and Order_Status = 'COMPLETE')
group by Delivery_Status
order by Count(distinct Customer_Id) desc;

select Delivery_Status, Count(distinct Customer_Id)
from co_supply
where Order_year = '2016'
and Order_Status = 'COMPLETE'
and customer_id not in (select customer_id from co_supply where order_year = 2017 and Order_Status = 'COMPLETE')
group by Delivery_Status
order by Count(distinct Customer_Id) desc;

Select count(distinct Customer_Id)
from co_supply
where Order_year= '2015'
and Order_Status = 'COMPLETE'
and Customer_id not in (select customer_id from co_supply where order_year = 2016 and Order_Status = 'COMPLETE');

#12 Calculate the percentage of Late delivery by each shipping mode

select Shipping_Mode, count(distinct Order_Id) as 'Number of Order'
from co_supply
where Order_Status = 'COMPLETE'
and Delivery_Status = 'Late Delivery'
group by Shipping_Mode
order by count(distinct Order_Id) desc; #Calculate the number of orders that are deliveried late for each shipping mode

select Shipping_Mode, count(distinct Order_Id) as 'Number of Order'
from co_supply
where Order_Status = 'COMPLETE'
group by Shipping_Mode
order by count(distinct Order_Id) desc; # Calculate the number of complete orders that for each shipping mode

#=> First Class SHipping mode has 100% orders that are deliveried late

select * from co_supply 
where Shipping_Mode = 'First Class';

#13 Calcualate the percentage of shipping Cancenled

select Delivery_Status,count(distinct Order_Id) AS 'Number of orders', 
(count(distinct Order_Id) / ( SELECT count(distinct Order_Id) FROM co_supply) * 100 ) AS  '% of orders'
from co_supply
group by Delivery_Status
order by count(distinct Order_Id) desc;
#=> Only 4.3% orders are cancenled

#14 Percentage number of Orders out of USA
select * from co_supply;

select count(distinct Order_id) as 'Number of orders out of USA', (select count(distinct Order_id) 
from co_supply) as 'Total Number of Orders', (count(distinct Order_id)/(select count(distinct Order_id) 
from co_supply)) '% Number of Orders out of USA'
from co_supply
where Order_Country != 'USA'; # Calculate Number of Orders out of USA

#15 Number of Orders by Type of Payment

select Type, count(distinct Order_id) as 'Number of orders'
from co_supply
group by Type
order by count(distinct Order_id) desc;
#=> Debit payment method is the most used method

#16 Order_Item_Quantity vs Order_Item_Discount_Rate

select min(Order_Item_Discount_Rate), max(Order_Item_Discount_Rate)
from co_supply;

alter table co_supply
add column Discount_Rate_Category decimal(3,2);

update co_supply
set Discount_Rate_Category = case
when Order_Item_Discount_Rate = 0 then 0
when Order_Item_Discount_Rate <= 0.05 and  Order_Item_Discount_Rate > 0 then 0.05
when Order_Item_Discount_Rate <= 0.1 and  Order_Item_Discount_Rate > 0.05 then 0.1
when Order_Item_Discount_Rate <= 0.15 and  Order_Item_Discount_Rate > 0.1 then 0.15
when Order_Item_Discount_Rate <= 0.2 and  Order_Item_Discount_Rate > 0.15 then 0.2
when Order_Item_Discount_Rate <= 0.25 and  Order_Item_Discount_Rate > 0.2 then 0.25
end;

select Discount_Rate_Category, sum(Order_Item_Quantity) as 'Item Quatity', sum(Sales_per_customer) as 'Revenue', sum(Benefit_per_order) as 'Benefit'
from co_supply
group by Discount_Rate_Category
order by sum(Order_Item_Quantity) desc; 
#=> Discounting Product price make more sales

select * from co_supply
where Delivery_Status ='Shipping Canceled';

select * from co_supply;

# 17 Revenue, Benefit, Orders loss through Year

select Order_Year,sum(Sales_per_customer) as 'Revenue' ,sum(Benefit_per_order) as 'Benefit', count(distinct Order_Id) as 'Number of orders'
from co_supply
where Order_Status = 'SUSPECTED_FRAUD' or Order_Status = 'CANCELED' 
group by Order_Year
order by Order_Year;

select Product_Name,Category_Name,sum(Sales_per_customer) as 'Revenue' ,sum(Benefit_per_order) as 'Benefit', sum(Order_Item_Quantity) as 'Item Quantity'
from co_supply
where Order_Status = 'SUSPECTED_FRAUD' or Order_Status = 'CANCELED' 
group by Category_Name, Product_Name
order by sum(Order_Item_Quantity) desc; #Identify top products that lost sales

# 18 SUSPECTED_FRAUD Orders

select Order_Year, count(distinct Order_Id) as 'Number of orders'
from co_supply
where Order_Status = 'SUSPECTED_FRAUD'
group by Order_Year
order by Order_Year; #Calculate USPECTED_FRAUD Orders through years

select Order_Region, count(distinct Order_Id) as 'Number of orders', 
(count(distinct Order_Id)/(select count(distinct Order_Id) from co_supply where Order_Status = 'SUSPECTED_FRAUD')) as '% of SUSPECTED FRAUD Orders' 
from co_supply
where Order_Status = 'SUSPECTED_FRAUD'
group by Order_Region
order by count(distinct Order_Id) desc; #Calculate USPECTED_FRAUD Orders by Region
#=> Western Europ, Central America, South America, Oceania, Southeast Asia are top Region that haves the most suspected fraud orders

select Category_Name, count(distinct Order_Id) as 'Number of orders', 
(count(distinct Order_Id)/(select count(distinct Order_Id) from co_supply where Order_Status = 'SUSPECTED_FRAUD')) as '% of SUSPECTED FRAUD Orders' 
from co_supply
where Order_Status = 'SUSPECTED_FRAUD'
group by Category_Name
order by count(distinct Order_Id) desc;













