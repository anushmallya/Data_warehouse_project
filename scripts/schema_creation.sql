/*
------------------------------------------
-- Create Schemas 'Bronze,Silver,Gold'
------------------------------------------
Script Purpose:
  This script sets up three schemas within the database: 'bronze','silver','gold'.

*/

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'bronze')
BEGIN
  EXEC('CREATE SCHEMA bronze');
END
GO
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'silver')
BEGIN
  EXEC('CREATE SCHEMA silver');
END
GO
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
BEGIN
  EXEC('CREATE SCHEMA gold');
END
