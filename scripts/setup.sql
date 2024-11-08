/***************************************************************************************************
         
Quickstart:   Streamlining Support Case Analysis with Snowflake Cortex 
Version:      v1
Author:       Kala Govindarajan
Copyright(c): 2024 Snowflake Inc. All rights reserved.
****************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author              Comments
------------------- ------------------- ------------------------------------------------------------
2024-10-26          Kala Govindarajan      Initial Release
***************************************************************************************************/

--STEP 1 Setup Database, Schema, role, warehouse and tables

USE ROLE SYSADMIN;

/*--
 • database, schema and warehouse creation
--*/

-- create a database
CREATE OR REPLACE DATABASE TICKETS_DB;

CREATE OR REPLACE SCHEMA TICKETS_DB.SUPPORT_SCHEMA;

CREATE OR REPLACE WAREHOUSE SUPPORT_WH 
WAREHOUSE_SIZE='X-LARGE';

CREATE OR REPLACE NETWORK RULE allow_all_rule
MODE= 'EGRESS'
TYPE = 'HOST_PORT'
VALUE_LIST = ('0.0.0.0:443','0.0.0.0:80');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION allow_all_integration
ALLOWED_NETWORK_RULES = (allow_all_rule)
ENABLED = true;

CREATE OR REPLACE NETWORK RULE pypi_network_rule
MODE = EGRESS
TYPE = HOST_PORT
VALUE_LIST = ('pypi.org', 'pypi.python.org', 'pythonhosted.org',  'files.pythonhosted.org');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION pypi_accessintegration
ALLOWED_NETWORK_RULES = (pypi_network_rule)
ENABLED = true;

CREATE OR REPLACE NETWORK RULE hf_network_rule
          TYPE = HOST_PORT
          MODE = EGRESS
          VALUE_LIST = ('huggingface.co', 'cdn-lfs.huggingface.co');

          
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION hf_access_integration2
        ALLOWED_NETWORK_RULES = (hf_network_rule)
        ENABLED = TRUE;


-- create roles
USE ROLE securityadmin;

-- functional roles

CREATE ROLE IF NOT EXISTS SUPPORT_ROLE COMMENT = 'SUPPORT role';
GRANT ROLE SUPPORT_ROLE TO role SYSADMIN;

--Grants

GRANT USAGE ON DATABASE TICKETS_DB TO ROLE SUPPORT_ROLE;
GRANT USAGE ON ALL SCHEMAS IN DATABASE TICKETS_DB TO ROLE SUPPORT_ROLE;

GRANT ALL ON SCHEMA TICKETS_DB.SUPPORT_SCHEMA TO ROLE SUPPORT_ROLE;
GRANT ALL ON WAREHOUSE SUPPORT_WH TO ROLE SUPPORT_ROLE;

-- future grants
GRANT ALL ON FUTURE TABLES IN SCHEMA TICKETS_DB.SUPPORT_SCHEMA TO ROLE SUPPORT_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA TICKETS_DB.SUPPORT_SCHEMA TO ROLE SUPPORT_ROLE;

use role SYSADMIN;
GRANT USAGE ON INTEGRATION allow_all_integration TO ROLE SUPPORT_ROLE;
GRANT USAGE ON INTEGRATION pypi_accessintegration TO ROLE SUPPORT_ROLE;
GRANT USAGE ON INTEGRATION hf_access_integration2 TO ROLE SUPPORT_ROLE;

--- End of the Setup from Snowsight SQL Worksheet. Proceed on to the Notebook

