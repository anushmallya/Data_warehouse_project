/*
--------------------------------------------
-- Creating a DataBase 'Data Warehouse'
--------------------------------------------
Script Purpose:
  This Script creates a new database named ''DataWarehouse'' 

*/

IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'DateWarehouse')
BEGIN
  CREATE DATABASE DateWarehouse;
END
