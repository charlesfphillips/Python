#!/usr/bin/env python3
"""
Database Schema Documenter
--------------------------
A tool to document database schemas for MSSQL and DB2 databases.
Generates documentation in various formats including Markdown, Excel, CSV, and JSON.
"""
import pyodbc
import traceback
import os
import sys
import argparse
import json
import csv
import re
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime

# Try to import database drivers
try:
    import ibm_db
    import ibm_db_dbi
    HAS_IBM_DB = True
except ImportError:
    HAS_IBM_DB = False
    print("IBM DB2 driver (ibm_db) not available. To install, run: pip install ibm_db ibm_db_sa")

try:
    import pyodbc
    HAS_PYODBC = True
except ImportError:
    HAS_PYODBC = False
    print("PyODBC driver not available. To install, run: pip install pyodbc")

try:
    import jaydebeapi
    HAS_JAYDEBEAPI = True
except ImportError:
    HAS_JAYDEBEAPI = False
    print("JayDeBeApi driver not available. To install, run: pip install jaydebeapi")

try:
    import pandas as pd
    import openpyxl
    EXCEL_AVAILABLE = True
except ImportError:
    EXCEL_AVAILABLE = False

try:
    import pydot
    DIAGRAM_AVAILABLE = True
except ImportError:
    DIAGRAM_AVAILABLE = False

class DatabaseDocumenter:
    def __init__(self, connection_string: str, db_type: str = 'mssql', graphviz_path: Optional[str] = None,
                 username: str = None, password: str = None, jdbc_url: str = None, schema: str = None):
        """Initialize the documenter with a connection string"""
        self.connection_string = connection_string
        self.db_type = db_type.lower()
        self.connection = None
        self.cursor = None
        self.graphviz_path = graphviz_path
        self.username = username
        self.password = password
        self.jdbc_url = jdbc_url
        self.schema = schema
        self.connection_successful = False
        
        # Parse server and database from connection string
        server = ""
        database = ""
        
        # Check if it's a JDBC URL for DB2
        if self.db_type == 'db2' and jdbc_url and jdbc_url.startswith('jdbc:db2://'):
            print(f"Using JDBC URL: {jdbc_url}")
            # Parse JDBC URL: jdbc:db2://hostname:port/database:params
            try:
                # Remove jdbc:db2:// prefix
                conn_parts = jdbc_url.replace('jdbc:db2://', '').split('/')
                host_port = conn_parts[0].split(':')
                server = host_port[0]
                
                # Handle database and parameters
                if len(conn_parts) > 1:
                    db_params = conn_parts[1].split(':')
                    database = db_params[0]
                    
                    # Extract additional parameters if present
                    self.jdbc_params = ""
                    if len(db_params) > 1:
                        self.jdbc_params = db_params[1]
                else:
                    database = ""
                    self.jdbc_params = ""
                
                # Store the parsed JDBC URL
                self.is_jdbc_url = True
                self.jdbc_server = server
                self.jdbc_port = host_port[1] if len(host_port) > 1 else '50000'
                self.jdbc_database = database
                
                print(f"Parsed JDBC URL - Server: {server}, Port: {self.jdbc_port}, Database: {database}")
                if self.jdbc_params:
                    print(f"Additional parameters: {self.jdbc_params}")
            except Exception as e:
                print(f"Error parsing JDBC URL: {str(e)}")
                # Continue with regular parsing as fallback
                self.is_jdbc_url = False
        else:
            self.is_jdbc_url = False
        
        if self.db_type == 'mssql':
            # Extract SERVER and DATABASE from connection string
            server_match = re.search(r'SERVER=([^;]+)', connection_string, re.IGNORECASE)
            db_match = re.search(r'DATABASE=([^;]+)', connection_string, re.IGNORECASE)
            
            if server_match:
                server = server_match.group(1)
            if db_match:
                database = db_match.group(1)
        elif self.db_type == 'db2':
            # Extract HOSTNAME and DATABASE from connection string
            server_match = re.search(r'HOSTNAME=([^;]+)', connection_string, re.IGNORECASE)
            db_match = re.search(r'DATABASE=([^;]+)', connection_string, re.IGNORECASE)
            
            if not server_match:
                # Try alternative format
                server_match = re.search(r'Hostname=([^;]+)', connection_string, re.IGNORECASE)
            
            if not db_match:
                # Try alternative format
                db_match = re.search(r'Database=([^;]+)', connection_string, re.IGNORECASE)
            
            if server_match:
                server = server_match.group(1)
            if db_match:
                database = db_match.group(1)
        
        # Initialize schema data structure
        self.schema_data = {
            'server': server,
            'database': database,
            'schema': schema,
            'extracted_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'schemas': [],
            'tables': [],
            'views': [],
            'procedures': [],
            'functions': [],
            'relationships': [],
            'indexes': []
        }
        
        # Connect to the database
        if self.db_type == 'db2':
            self._connect_db2_with_fallbacks(jdbc_url, username, password)
        else:
            try:
                print(f"Connecting to {db_type} database...")
                self.connection = pyodbc.connect(connection_string)
                self.cursor = self.connection.cursor()
                print("Connection successful!")
                self.connection_successful = True
            except pyodbc.Error as e:
                error_code = e.args[0] if len(e.args) > 0 else "Unknown"
                error_message = e.args[1] if len(e.args) > 1 else str(e)
                print(f"Connection Error ({error_code}): {error_message}")
                raise ConnectionError(f"Failed to connect to the database: {error_message}")
            except Exception as e:
                print(f"Unexpected error: {str(e)}")
                raise ConnectionError(f"Failed to connect to the database: {str(e)}")

    def _connect_db2_with_fallbacks(self, jdbc_url, username, password):
        """Connect to DB2 using multiple methods with fallbacks"""
        print("Connecting to db2 database...")
        
        # Track connection attempts
        connection_attempts = []
        
        # 1. Try JayDeBeApi with JDBC driver
        if HAS_JAYDEBEAPI and jdbc_url:
            try:
                print("Attempting to connect using JayDeBeApi with JDBC driver...")
                
                # Find DB2 JDBC driver jar
                jdbc_driver_paths = [
                    "C:\\Program Files\\IBM\\SDPShared\\plugins\\com.ibm.datatools.db2_2.2.0.v20130525_0720\\driver\\db2jcc.jar",
                    "C:\\Program Files\\IBM\\SQLLIB\\java\\db2jcc.jar",
                    "C:\\Program Files\\IBM\\SQLLIB\\java\\db2jcc4.jar",
                    "C:\\IBM\\SQLLIB\\java\\db2jcc.jar",
                    "C:\\IBM\\SQLLIB\\java\\db2jcc4.jar",
                    # Add the current directory and subdirectories
                    os.path.join(os.getcwd(), "db2jcc.jar"),
                    os.path.join(os.getcwd(), "db2jcc4.jar"),
                    os.path.join(os.getcwd(), "lib", "db2jcc.jar"),
                    os.path.join(os.getcwd(), "lib", "db2jcc4.jar"),
                    os.path.join(os.getcwd(), "drivers", "db2jcc.jar"),
                    os.path.join(os.getcwd(), "drivers", "db2jcc4.jar"),
                ]
                
                # Check if DB2DRIVER_PATH environment variable is set
                if 'DB2DRIVER_PATH' in os.environ:
                    jdbc_driver_paths.insert(0, os.environ['DB2DRIVER_PATH'])
                
                # Find the first existing driver jar
                jdbc_driver_path = None
                for path in jdbc_driver_paths:
                    if os.path.exists(path):
                        print(f"Found JDBC driver at: {path}")
                        jdbc_driver_path = path
                        break
                
                if not jdbc_driver_path:
                    print("Warning: DB2 JDBC driver jar not found in common locations.")
                    print("Please download db2jcc.jar or db2jcc4.jar and place it in the current directory")
                    print("or set the DB2DRIVER_PATH environment variable to its location.")
                    print("Trying to connect without specifying the driver path...")
                    
                    # Try to connect without specifying the driver path
                    # This might work if the driver is in the Java classpath
                    self.connection = jaydebeapi.connect(
                        "com.ibm.db2.jcc.DB2Driver",
                        jdbc_url,
                        [username, password]
                    )
                else:
                    # Connect using JayDeBeApi with the found driver
                    self.connection = jaydebeapi.connect(
                        "com.ibm.db2.jcc.DB2Driver",
                        jdbc_url,
                        [username, password],
                        jdbc_driver_path
                    )
                
                self.cursor = self.connection.cursor()
                print("Connection successful using JayDeBeApi with JDBC driver!")
                
                # Set current schema if specified
                if self.schema:
                    try:
                        print(f"Setting current schema to: {self.schema}")
                        self.cursor.execute(f"SET CURRENT SCHEMA = {self.schema}")
                        print(f"Schema set successfully to {self.schema}")
                    except Exception as e:
                        print(f"Warning: Failed to set schema: {str(e)}")
                
                self.connection_successful = True
                return
            except Exception as e:
                error_msg = str(e)
                connection_attempts.append(f"JayDeBeApi error: {error_msg}")
                
                # Special handling for authentication errors
                if "invalid authorization" in error_msg.lower() or "authorization failure" in error_msg.lower():
                    print("Authentication failed. Please check your username and password.")
                else:
                    print(f"Error connecting with JayDeBeApi: {error_msg}")
                
                print("Falling back to other connection methods...")
        # 2. Try IBM DB2 native driver
        if IBM_DB_AVAILABLE:
            try:
                print("Attempting to connect using IBM DB2 native driver...")
                
                # Parse JDBC URL if provided
                if jdbc_url:
                    # Handle complex JDBC URLs with parameters
                    # Format: jdbc:db2://hostname:port/database:params
                    match = re.match(r'jdbc:db2://([^:]+):(\d+)/([^:]+)(?::(.*))?', jdbc_url)
                    if match:
                        server, port, database, params = match.groups()
                        
                        # Create ibm_db connection string
                        conn_string = (
                            f"DATABASE={database};"
                            f"HOSTNAME={server};"
                            f"PORT={port};"
                            f"PROTOCOL=TCPIP;"
                            f"UID={username};"
                            f"PWD={password};"
                        )
                        
                        # Add schema if specified
                        if self.schema:
                            conn_string += f"CURRENTSCHEMA={self.schema};"
                        
                        # Connect using ibm_db
                        conn_id = ibm_db.connect(conn_string, "", "")
                        self.connection = ibm_db_dbi.Connection(conn_id)
                        self.cursor = self.connection.cursor()
                        
                        # Set current schema if specified and not already set in connection string
                        if self.schema and "CURRENTSCHEMA" not in conn_string.upper():
                            try:
                                print(f"Setting current schema to: {self.schema}")
                                self.cursor.execute(f"SET CURRENT SCHEMA = {self.schema}")
                                print(f"Schema set successfully to {self.schema}")
                            except Exception as e:
                                print(f"Warning: Failed to set schema: {str(e)}")
                        
                        print("Connection successful using IBM DB2 native driver!")
                        self.connection_successful = True
                        return
                    else:
                        connection_attempts.append("Failed to parse JDBC URL for IBM DB2 driver")
                elif self.connection_string:
                    # Use the provided connection string
                    conn_id = ibm_db.connect(self.connection_string, "", "")
                    self.connection = ibm_db_dbi.Connection(conn_id)
                    self.cursor = self.connection.cursor()
                    
                    # Set current schema if specified
                    if self.schema:
                        try:
                            print(f"Setting current schema to: {self.schema}")
                            self.cursor.execute(f"SET CURRENT SCHEMA = {self.schema}")
                            print(f"Schema set successfully to {self.schema}")
                        except Exception as e:
                            print(f"Warning: Failed to set schema: {str(e)}")
                    
                    print("Connection successful using IBM DB2 native driver with connection string!")
                    self.connection_successful = True
                    return
            except Exception as e:
                error_msg = str(e)
                connection_attempts.append(f"IBM DB2 driver error: {error_msg}")
                print(f"Error connecting with IBM DB2 driver: {error_msg}")
                print("Falling back to ODBC...")
        else:
            connection_attempts.append("IBM DB2 driver (ibm_db) not available")
        
        # 3. Try ODBC with DB2 driver
        if PYODBC_AVAILABLE:
            try:
                print("Attempting to connect using ODBC...")
                
                # List available drivers
                drivers = [driver for driver in pyodbc.drivers()]
                db2_drivers = [driver for driver in drivers if 'DB2' in driver]
                
                if db2_drivers:
                    # Use the first DB2 driver found
                    driver = db2_drivers[0]
                    
                    # Parse JDBC URL if provided
                    if jdbc_url:
                        # Handle complex JDBC URLs with parameters
                        match = re.match(r'jdbc:db2://([^:]+):(\d+)/([^:]+)(?::(.*))?', jdbc_url)
                        if match:
                            server, port, database, params = match.groups()
                            
                            # Create ODBC connection string
                            odbc_conn_string = f"DRIVER={{{driver}}};DATABASE={database};HOSTNAME={server};PORT={port};PROTOCOL=TCPIP;"
                            
                            if username:
                                odbc_conn_string += f"UID={username};"
                            if password:
                                odbc_conn_string += f"PWD={password};"
                            
                            # Add schema if specified
                            if self.schema:
                                odbc_conn_string += f"CURRENTSCHEMA={self.schema};"
                            
                            # Connect using pyodbc
                            self.connection = pyodbc.connect(odbc_conn_string)
                            self.cursor = self.connection.cursor()
                            
                            # Set current schema if specified and not already set in connection string
                            if self.schema and "CURRENTSCHEMA" not in odbc_conn_string.upper():
                                try:
                                    print(f"Setting current schema to: {self.schema}")
                                    self.cursor.execute(f"SET CURRENT SCHEMA = {self.schema}")
                                    print(f"Schema set successfully to {self.schema}")
                                except Exception as e:
                                    print(f"Warning: Failed to set schema: {str(e)}")
                            
                            print("Connection successful using ODBC!")
                            self.connection_successful = True
                            return
                        else:
                            connection_attempts.append("Failed to parse JDBC URL for ODBC")
                    elif self.connection_string:
                        # Use the provided connection string
                        self.connection = pyodbc.connect(self.connection_string)
                        self.cursor = self.connection.cursor()
                        
                        # Set current schema if specified
                        if self.schema:
                            try:
                                print(f"Setting current schema to: {self.schema}")
                                self.cursor.execute(f"SET CURRENT SCHEMA = {self.schema}")
                                print(f"Schema set successfully to {self.schema}")
                            except Exception as e:
                                print(f"Warning: Failed to set schema: {str(e)}")
                        
                        print("Connection successful using ODBC with connection string!")
                        self.connection_successful = True
                        return
                else:
                    connection_attempts.append("No DB2 ODBC drivers found")
                    print("No DB2 ODBC drivers found. Available drivers:")
                    for driver in drivers:
                        print(f"  - {driver}")
            except Exception as e:
                error_msg = str(e)
                connection_attempts.append(f"ODBC error: {error_msg}")
                print(f"Error connecting with ODBC: {error_msg}")
        else:
            connection_attempts.append("PyODBC not available")
        
        # If we got here, all connection methods failed
        error_message = "Failed to connect to DB2 database. Attempted methods:\n"
        for attempt in connection_attempts:
            error_message += f"  - {attempt}\n"
        
        print(error_message)
        raise ConnectionError(error_message)

    def disconnect(self) -> None:
        """Disconnect from the database"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def _execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute a query and return results as a list of dictionaries"""
        try:
            if isinstance(self.connection, jaydebeapi.Connection):
                # This is a JayDeBeApi connection
                self.cursor.execute(query)
                
                # Get column names
                columns = [column[0] for column in self.cursor.description]
                
                # Fetch all rows and convert to dictionaries
                results = []
                for row in self.cursor.fetchall():
                    results.append(dict(zip(columns, row)))
                
                return results
            elif self.db_type == 'db2' and hasattr(self.connection, '_conn_handle'):
                # This is an ibm_db connection
                import ibm_db
                
                # Execute the query directly with ibm_db
                stmt = ibm_db.exec_immediate(self.connection._conn_handle, query)
                
                # Get column information
                columns = []
                col_count = ibm_db.num_fields(stmt)
                
                for i in range(col_count):
                    col_name = ibm_db.field_name(stmt, i)
                    columns.append(col_name)
                
                # Fetch the results
                results = []
                row = ibm_db.fetch_assoc(stmt)
                
                while row:
                    # Convert to a regular dictionary
                    row_dict = {k.upper(): v for k, v in row.items()}
                    results.append(row_dict)
                    row = ibm_db.fetch_assoc(stmt)
                
                return results
            else:
                # Regular pyodbc cursor
                self.cursor.execute(query)
                
                # Get column names
                columns = [column[0] for column in self.cursor.description]
                
                # Fetch all rows and convert to dictionaries
                results = []
                for row in self.cursor.fetchall():
                    results.append(dict(zip(columns, row)))
                
                return results
        except Exception as e:
            print(f"Error executing query: {str(e)}")
            print(f"Query: {query}")
            return []

    def _extract_schema(self) -> Dict[str, Any]:
        """Extract schema information from the database"""
        if not self.connection:
            raise ConnectionError("Not connected to database")
        
        print("Extracting database schema...")
        
        # Extract schemas
        self._extract_schemas()
        
        # Extract tables
        self._extract_tables()
        
        # Extract views
        self._extract_views()
        
        # Extract relationships
        self._extract_relationships()
        
        # Extract stored procedures
        self._extract_procedures()
        
        # Extract functions
        self._extract_functions()
        
        # Extract indexes
        self._extract_indexes()
        
        return self.schema_data

    def extract_schema(self) -> dict:
        """Public method to extract the database schema"""
        return self._extract_schema()
    
    def _extract_schemas(self) -> None:
        """Extract schema information based on database type"""
        if self.db_type == 'mssql':
            self._extract_schemas_mssql()
        elif self.db_type == 'db2':
            self._extract_schemas_db2()
    
    def _extract_schemas_mssql(self) -> None:
        """Extract schema information from MS SQL Server"""
        try:
            self.cursor.execute("""
                SELECT 
                    s.name AS schema_name,
                    s.schema_id,
                    ISNULL(p.name, 'dbo') AS owner_name,
                    ISNULL(ep.value, '') AS description
                FROM 
                    sys.schemas s
                LEFT JOIN 
                    sys.database_principals p ON s.principal_id = p.principal_id
                LEFT JOIN 
                    sys.extended_properties ep ON ep.major_id = s.schema_id
                    AND ep.minor_id = 0
                    AND ep.class = 3
                    AND ep.name = 'MS_Description'
                WHERE 
                    s.name NOT IN ('sys', 'INFORMATION_SCHEMA', 'guest')
                ORDER BY 
                    s.name
            """)
            
            schemas = self.cursor.fetchall()
            
            for schema_name, schema_id, owner_name, description in schemas:
                schema_info = {
                    'name': schema_name,
                    'id': schema_id,
                    'owner': owner_name,
                    'description': description
                }
                
                self.schema_data['schemas'].append(schema_info)
            
        except Exception as e:
            print(f"Error extracting schemas from MSSQL: {str(e)}")

    def _extract_schemas_db2(self) -> None:
        """Extract schema information from DB2"""
        try:
            self.cursor.execute("""
                SELECT 
                    SCHEMANAME,
                    OWNER,
                    CREATE_TIME,
                    DEFINER
                FROM 
                    SYSCAT.SCHEMATA
                WHERE 
                    SCHEMANAME NOT LIKE 'SYS%'
                    AND SCHEMANAME NOT IN ('NULLID', 'SQLJ')
                ORDER BY 
                    SCHEMANAME
            """)
            
            schemas = self.cursor.fetchall()
            
            for schema_name, owner, create_time, definer in schemas:
                schema_info = {
                    'name': schema_name.strip(),
                    'owner': owner.strip(),
                    'create_time': create_time.strip() if create_time else None,
                    'definer': definer.strip() if definer else None,
                    'description': ''  # DB2 doesn't store schema descriptions in the same way
                }
                
                self.schema_data['schemas'].append(schema_info)
            
        except Exception as e:
            print(f"Error extracting schemas from DB2: {str(e)}")
    
    def _extract_tables(self) -> None:
        """Extract table information"""
        if self.db_type == 'mssql':
            self._extract_tables_mssql()
        elif self.db_type == 'db2':
            self._extract_tables_db2()

    def _extract_tables_mssql(self) -> None:
        """Extract table information from MS SQL Server"""
        try:
            # Get tables
            self.cursor.execute("""
                SELECT 
                    t.name AS table_name,
                    s.name AS schema_name,
                    t.object_id,
                    t.create_date,
                    t.modify_date,
                    ISNULL(ep.value, '') AS description
                FROM 
                    sys.tables t
                INNER JOIN 
                    sys.schemas s ON t.schema_id = s.schema_id
                LEFT JOIN 
                    sys.extended_properties ep ON ep.major_id = t.object_id
                    AND ep.minor_id = 0
                    AND ep.class = 1
                    AND ep.name = 'MS_Description'
                ORDER BY 
                    s.name, t.name
            """)
            
            tables = self.cursor.fetchall()
            
            for table_name, schema_name, object_id, create_date, modify_date, description in tables:
                # Skip system tables
                if schema_name in ('sys', 'INFORMATION_SCHEMA'):
                    continue
                
                # Get columns for this table
                self.cursor.execute("""
                    SELECT 
                        c.name AS column_name,
                        t.name AS data_type,
                        c.max_length,
                        c.precision,
                        c.scale,
                        c.is_nullable,
                        c.is_identity,
                        ISNULL(ep.value, '') AS description,
                        CASE WHEN pk.column_id IS NOT NULL THEN 1 ELSE 0 END AS is_primary_key,
                        c.column_id AS ordinal_position,
                        c.default_object_id,
                        dc.definition AS default_value
                    FROM 
                        sys.columns c
                    INNER JOIN 
                        sys.types t ON c.user_type_id = t.user_type_id
                    LEFT JOIN 
                        sys.extended_properties ep ON ep.major_id = c.object_id
                        AND ep.minor_id = c.column_id
                        AND ep.class = 1
                        AND ep.name = 'MS_Description'
                    LEFT JOIN (
                        SELECT 
                            ic.column_id, ic.object_id
                        FROM 
                            sys.index_columns ic
                        INNER JOIN 
                            sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                        WHERE 
                            i.is_primary_key = 1
                    ) pk ON pk.column_id = c.column_id AND pk.object_id = c.object_id
                    LEFT JOIN 
                        sys.default_constraints dc ON c.default_object_id = dc.object_id
                    WHERE 
                        c.object_id = ?
                    ORDER BY 
                        c.column_id
                """, (object_id,))
                
                columns = self.cursor.fetchall()
                
                table_columns = []
                primary_keys = []
                
                for column_name, data_type, max_length, precision, scale, is_nullable, is_identity, col_description, is_primary_key, ordinal_position, default_object_id, default_value in columns:
                    # Format data type
                    formatted_data_type = data_type
                    if data_type in ('varchar', 'nvarchar', 'char', 'nchar'):
                        if max_length == -1:
                            formatted_data_type += '(MAX)'
                        else:
                            if data_type in ('nvarchar', 'nchar'):
                                max_length = max_length // 2
                            formatted_data_type += f'({max_length})'
                    elif data_type in ('decimal', 'numeric'):
                        formatted_data_type += f'({precision}, {scale})'
                    
                    column_info = {
                        'name': column_name,
                        'data_type': formatted_data_type,
                        'is_nullable': bool(is_nullable),
                        'is_identity': bool(is_identity),
                        'description': col_description,
                        'ordinal_position': ordinal_position,
                        'default_value': default_value
                    }
                    
                    table_columns.append(column_info)
                    
                    if is_primary_key:
                        primary_keys.append(column_name)
                
                table_info = {
                    'name': table_name,
                    'schema': schema_name,
                    'description': description,
                    'create_date': create_date.isoformat() if create_date else None,
                    'modify_date': modify_date.isoformat() if modify_date else None,
                    'columns': table_columns,
                    'primary_keys': primary_keys
                }
                
                self.schema_data['tables'].append(table_info)
            
        except Exception as e:
            print(f"Error extracting tables from MSSQL: {str(e)}")

    def _extract_tables_db2(self) -> None:
        """Extract table information from DB2"""
        try:
            # Get tables
            query = """
                SELECT 
                    TABSCHEMA, 
                    TABNAME, 
                    CREATE_TIME, 
                    ALTER_TIME, 
                    REMARKS
                FROM 
                    SYSCAT.TABLES
                WHERE 
                    TYPE = 'T'
                    AND TABSCHEMA NOT LIKE 'SYS%'
                    AND TABSCHEMA NOT IN ('NULLID', 'SQLJ')
            """
            
            if self.schema:
                query += f" AND TABSCHEMA = '{self.schema}'"
            
            query += " ORDER BY TABSCHEMA, TABNAME"
            
            tables = self._execute_query(query)
            
            for table in tables:
                schema_name = table.get('TABSCHEMA', '').strip()
                table_name = table.get('TABNAME', '').strip()
                
                # Get columns for this table
                columns_query = f"""
                    SELECT 
                        COLNAME, 
                        TYPENAME, 
                        LENGTH, 
                        SCALE, 
                        NULLS, 
                        IDENTITY, 
                        REMARKS, 
                        KEYSEQ, 
                        COLNO, 
                        DEFAULT
                    FROM 
                        SYSCAT.COLUMNS
                    WHERE 
                        TABSCHEMA = '{schema_name}'
                        AND TABNAME = '{table_name}'
                    ORDER BY 
                        COLNO
                """
                
                columns = self._execute_query(columns_query)
                
                table_columns = []
                primary_keys = []
                
                for column in columns:
                    column_name = column.get('COLNAME', '').strip()
                    data_type = column.get('TYPENAME', '').strip()
                    length = column.get('LENGTH')
                    scale = column.get('SCALE')
                    is_nullable = column.get('NULLS', '').strip() == 'Y'
                    is_identity = column.get('IDENTITY', '').strip() == 'Y'
                    description = column.get('REMARKS', '').strip()
                    key_seq = column.get('KEYSEQ')
                    ordinal_position = column.get('COLNO')
                    default_value = column.get('DEFAULT', '').strip()
                    
                    # Format data type
                    formatted_data_type = data_type
                    if data_type in ('VARCHAR', 'CHAR', 'GRAPHIC', 'VARGRAPHIC'):
                        formatted_data_type += f'({length})'
                    elif data_type in ('DECIMAL'):
                        formatted_data_type += f'({length}, {scale})'
                    
                    column_info = {
                        'name': column_name,
                        'data_type': formatted_data_type,
                        'is_nullable': is_nullable,
                        'is_identity': is_identity,
                        'description': description,
                        'ordinal_position': ordinal_position,
                        'default_value': default_value
                    }
                    
                    table_columns.append(column_info)
                    
                    if key_seq is not None and key_seq > 0:
                        primary_keys.append(column_name)
                
                table_info = {
                    'name': table_name,
                    'schema': schema_name,
                    'description': table.get('REMARKS', '').strip(),
                    'create_date': table.get('CREATE_TIME', '').strip(),
                    'modify_date': table.get('ALTER_TIME', '').strip(),
                    'columns': table_columns,
                    'primary_keys': primary_keys
                }
                
                self.schema_data['tables'].append(table_info)
            
        except Exception as e:
            print(f"Error extracting tables from DB2: {str(e)}")

    def _extract_views(self) -> None:
        """Extract view information"""
        if self.db_type == 'mssql':
            self._extract_views_mssql()
        elif self.db_type == 'db2':
            self._extract_views_db2()
    
    def _extract_views_mssql(self) -> None:
        """Extract view information from MS SQL Server"""
        try:
            # Get views
            self.cursor.execute("""
                SELECT 
                    v.name AS view_name,
                    s.name AS schema_name,
                    v.object_id,
                    v.create_date,
                    v.modify_date,
                    ISNULL(ep.value, '') AS description,
                    m.definition AS view_definition
                FROM 
                    sys.views v
                INNER JOIN 
                    sys.schemas s ON v.schema_id = s.schema_id
                LEFT JOIN 
                    sys.extended_properties ep ON ep.major_id = v.object_id
                    AND ep.minor_id = 0
                    AND ep.class = 1
                    AND ep.name = 'MS_Description'
                LEFT JOIN 
                    sys.sql_modules m ON v.object_id = m.object_id
                ORDER BY 
                    s.name, v.name
            """)
            
            views = self.cursor.fetchall()
            
            for view_name, schema_name, object_id, create_date, modify_date, description, view_definition in views:
                # Skip system views
                if schema_name in ('sys', 'INFORMATION_SCHEMA'):
                    continue
                
                # Get columns for this view
                self.cursor.execute("""
                    SELECT 
                        c.name AS column_name,
                        t.name AS data_type,
                        c.max_length,
                        c.precision,
                        c.scale,
                        c.is_nullable,
                        ISNULL(ep.value, '') AS description,
                        c.column_id AS ordinal_position
                    FROM 
                        sys.columns c
                    INNER JOIN 
                        sys.types t ON c.user_type_id = t.user_type_id
                    LEFT JOIN 
                        sys.extended_properties ep ON ep.major_id = c.object_id
                        AND ep.minor_id = c.column_id
                        AND ep.class = 1
                        AND ep.name = 'MS_Description'
                    WHERE 
                        c.object_id = ?
                    ORDER BY 
                        c.column_id
                """, (object_id,))
                
                columns = self.cursor.fetchall()
                
                view_columns = []
                
                for column_name, data_type, max_length, precision, scale, is_nullable, col_description, ordinal_position in columns:
                    # Format data type
                    formatted_data_type = data_type
                    if data_type in ('varchar', 'nvarchar', 'char', 'nchar'):
                        if max_length == -1:
                            formatted_data_type += '(MAX)'
                        else:
                            if data_type in ('nvarchar', 'nchar'):
                                max_length = max_length // 2
                            formatted_data_type += f'({max_length})'
                    elif data_type in ('decimal', 'numeric'):
                        formatted_data_type += f'({precision}, {scale})'
                    
                    column_info = {
                        'name': column_name,
                        'data_type': formatted_data_type,
                        'is_nullable': bool(is_nullable),
                        'description': col_description,
                        'ordinal_position': ordinal_position
                    }
                    
                    view_columns.append(column_info)
                
                view_info = {
                    'name': view_name,
                    'schema': schema_name,
                    'description': description,
                    'create_date': create_date.isoformat() if create_date else None,
                    'modify_date': modify_date.isoformat() if modify_date else None,
                    'columns': view_columns,
                    'definition': view_definition
                }
                
                self.schema_data['views'].append(view_info)
            
        except Exception as e:
            print(f"Error extracting views from MSSQL: {str(e)}")

    def _extract_views_db2(self) -> None:
        """Extract view information from DB2"""
        try:
            # Get views
            query = """
                SELECT 
                    VIEWSCHEMA, 
                    VIEWNAME, 
                    CREATE_TIME, 
                    LAST_ALTERED, 
                    REMARKS,
                    TEXT AS VIEW_DEFINITION
                FROM 
                    SYSCAT.VIEWS
                WHERE 
                    VIEWSCHEMA NOT LIKE 'SYS%'
                    AND VIEWSCHEMA NOT IN ('NULLID', 'SQLJ')
            """
            
            if self.schema:
                query += f" AND VIEWSCHEMA = '{self.schema}'"
            
            query += " ORDER BY VIEWSCHEMA, VIEWNAME"
            
            views = self._execute_query(query)
            
            for view in views:
                schema_name = view.get('VIEWSCHEMA', '').strip()
                view_name = view.get('VIEWNAME', '').strip()
                
                # Get columns for this view
                columns_query = f"""
                    SELECT 
                        COLNAME, 
                        TYPENAME, 
                        LENGTH, 
                        SCALE, 
                        NULLS, 
                        REMARKS, 
                        COLNO
                    FROM 
                        SYSCAT.COLUMNS
                    WHERE 
                        TABSCHEMA = '{schema_name}'
                        AND TABNAME = '{view_name}'
                    ORDER BY 
                        COLNO
                """
                
                columns = self._execute_query(columns_query)
                
                view_columns = []
                
                for column in columns:
                    column_name = column.get('COLNAME', '').strip()
                    data_type = column.get('TYPENAME', '').strip()
                    length = column.get('LENGTH')
                    scale = column.get('SCALE')
                    is_nullable = column.get('NULLS', '').strip() == 'Y'
                    description = column.get('REMARKS', '').strip()
                    ordinal_position = column.get('COLNO')
                    
                    # Format data type
                    formatted_data_type = data_type
                    if data_type in ('VARCHAR', 'CHAR', 'GRAPHIC', 'VARGRAPHIC'):
                        formatted_data_type += f'({length})'
                    elif data_type in ('DECIMAL'):
                        formatted_data_type += f'({length}, {scale})'
                    
                    column_info = {
                        'name': column_name,
                        'data_type': formatted_data_type,
                        'is_nullable': is_nullable,
                        'description': description,
                        'ordinal_position': ordinal_position
                    }
                    
                    view_columns.append(column_info)
                
                view_info = {
                    'name': view_name,
                    'schema': schema_name,
                    'description': view.get('REMARKS', '').strip(),
                    'create_date': view.get('CREATE_TIME', '').strip(),
                    'modify_date': view.get('LAST_ALTERED', '').strip(),
                    'columns': view_columns,
                    'definition': view.get('VIEW_DEFINITION', '').strip()
                }
                
                self.schema_data['views'].append(view_info)
            
        except Exception as e:
            print(f"Error extracting views from DB2: {str(e)}")

    def _extract_relationships(self) -> None:
        """Extract relationship information"""
        if self.db_type == 'mssql':
            self._extract_relationships_mssql()
        elif self.db_type == 'db2':
            self._extract_relationships_db2()
    
    def _extract_relationships_mssql(self) -> None:
        """Extract relationship information from MS SQL Server"""
        try:
            # Get foreign keys
            self.cursor.execute("""
                SELECT 
                    fk.name AS constraint_name,
                    OBJECT_NAME(fk.parent_object_id) AS table_name,
                    SCHEMA_NAME(o.schema_id) AS table_schema,
                    COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS column_name,
                    OBJECT_NAME(fk.referenced_object_id) AS referenced_table_name,
                    SCHEMA_NAME(ro.schema_id) AS referenced_table_schema,
                    COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS referenced_column_name,
                    fk.delete_referential_action,
                    fk.update_referential_action
                FROM 
                    sys.foreign_keys fk
                INNER JOIN 
                    sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
                INNER JOIN 
                    sys.objects o ON fk.parent_object_id = o.object_id
                INNER JOIN 
                    sys.objects ro ON fk.referenced_object_id = ro.object_id
                ORDER BY 
                    table_schema, table_name, constraint_name
            """)
            
            foreign_keys = self.cursor.fetchall()
            
            # Group by constraint name
            constraints = {}
            
            for constraint_name, table_name, table_schema, column_name, referenced_table_name, referenced_table_schema, referenced_column_name, delete_action, update_action in foreign_keys:
                if constraint_name not in constraints:
                    constraints[constraint_name] = {
                        'name': constraint_name,
                        'table': table_name,
                        'schema': table_schema,
                        'columns': [],
                        'referenced_table': referenced_table_name,
                        'referenced_schema': referenced_table_schema,
                        'referenced_columns': [],
                        'delete_rule': self._get_referential_action_name(delete_action),
                        'update_rule': self._get_referential_action_name(update_action)
                    }
                
                constraints[constraint_name]['columns'].append(column_name)
                constraints[constraint_name]['referenced_columns'].append(referenced_column_name)
            
            # Add to schema data
            for constraint in constraints.values():
                self.schema_data['relationships'].append(constraint)
            
        except Exception as e:
            print(f"Error extracting relationships from MSSQL: {str(e)}")
    
    def _get_referential_action_name(self, action_id: int) -> str:
        """Convert referential action ID to name"""
        if action_id == 0:
            return 'NO ACTION'
        elif action_id == 1:
            return 'CASCADE'
        elif action_id == 2:
            return 'SET NULL'
        elif action_id == 3:
            return 'SET DEFAULT'
        else:
            return 'UNKNOWN'

    def _extract_relationships_db2(self) -> None:
        """Extract relationship information from DB2"""
        try:
            # Get foreign keys
            query = """
                SELECT 
                    CONSTNAME, 
                    TABSCHEMA, 
                    TABNAME, 
                    FK_COLNAMES, 
                    REFTABSCHEMA, 
                    REFTABNAME, 
                    PK_COLNAMES,
                    DELETERULE,
                    UPDATERULE
                FROM 
                    SYSCAT.REFERENCES
                WHERE 
                    TABSCHEMA NOT LIKE 'SYS%'
                    AND TABSCHEMA NOT IN ('NULLID', 'SQLJ')
            """
            
            if self.schema:
                query += f" AND TABSCHEMA = '{self.schema}'"
            
            query += " ORDER BY TABSCHEMA, TABNAME, CONSTNAME"
            
            foreign_keys = self._execute_query(query)
            
            for fk in foreign_keys:
                constraint_name = fk.get('CONSTNAME', '').strip()
                table_schema = fk.get('TABSCHEMA', '').strip()
                table_name = fk.get('TABNAME', '').strip()
                fk_columns = fk.get('FK_COLNAMES', '').strip()
                ref_schema = fk.get('REFTABSCHEMA', '').strip()
                ref_table = fk.get('REFTABNAME', '').strip()
                pk_columns = fk.get('PK_COLNAMES', '').strip()
                delete_rule = fk.get('DELETERULE', '').strip()
                update_rule = fk.get('UPDATERULE', '').strip()
                
                # Split column lists
                fk_column_list = [col.strip() for col in fk_columns.split(',')]
                pk_column_list = [col.strip() for col in pk_columns.split(',')]
                
                constraint = {
                    'name': constraint_name,
                    'table': table_name,
                    'schema': table_schema,
                    'columns': fk_column_list,
                    'referenced_table': ref_table,
                    'referenced_schema': ref_schema,
                    'referenced_columns': pk_column_list,
                    'delete_rule': delete_rule,
                    'update_rule': update_rule
                }
                
                self.schema_data['relationships'].append(constraint)
            
        except Exception as e:
            print(f"Error extracting relationships from DB2: {str(e)}")
    
    def _extract_procedures(self) -> None:
        """Extract stored procedure information"""
        if self.db_type == 'mssql':
            self._extract_procedures_mssql()
        elif self.db_type == 'db2':
            self._extract_procedures_db2()
    
    def _extract_procedures_mssql(self) -> None:
        """Extract stored procedure information from MS SQL Server"""
        try:
            # Get procedures
            self.cursor.execute("""
                SELECT 
                    p.name AS procedure_name,
                    s.name AS schema_name,
                    p.object_id,
                    p.create_date,
                    p.modify_date,
                    ISNULL(ep.value, '') AS description,
                    m.definition AS procedure_definition
                FROM 
                    sys.procedures p
                INNER JOIN 
                    sys.schemas s ON p.schema_id = s.schema_id
                LEFT JOIN 
                    sys.extended_properties ep ON ep.major_id = p.object_id
                    AND ep.minor_id = 0
                    AND ep.class = 1
                    AND ep.name = 'MS_Description'
                LEFT JOIN 
                    sys.sql_modules m ON p.object_id = m.object_id
                WHERE 
                    p.is_ms_shipped = 0
                ORDER BY 
                    s.name, p.name
            """)
            
            procedures = self.cursor.fetchall()
            
            for proc_name, schema_name, object_id, create_date, modify_date, description, proc_definition in procedures:
                # Get parameters for this procedure
                self.cursor.execute("""
                    SELECT 
                        p.name AS parameter_name,
                        t.name AS data_type,
                        p.max_length,
                        p.precision,
                        p.scale,
                        p.is_output,
                        p.has_default_value,
                        p.default_value,
                        p.parameter_id
                    FROM 
                        sys.parameters p
                    INNER JOIN 
                        sys.types t ON p.user_type_id = t.user_type_id
                    WHERE 
                        p.object_id = ?
                    ORDER BY 
                        p.parameter_id
                """, (object_id,))
                
                parameters = self.cursor.fetchall()
                
                proc_params = []
                
                for param_name, data_type, max_length, precision, scale, is_output, has_default, default_value, param_id in parameters:
                    # Format data type
                    formatted_data_type = data_type
                    if data_type in ('varchar', 'nvarchar', 'char', 'nchar'):
                        if max_length == -1:
                            formatted_data_type += '(MAX)'
                        else:
                            if data_type in ('nvarchar', 'nchar'):
                                max_length = max_length // 2
                            formatted_data_type += f'({max_length})'
                    elif data_type in ('decimal', 'numeric'):
                        formatted_data_type += f'({precision}, {scale})'
                    
                    param_info = {
                        'name': param_name,
                        'data_type': formatted_data_type,
                        'is_output': bool(is_output),
                        'has_default': bool(has_default),
                        'default_value': default_value,
                        'ordinal_position': param_id
                    }
                    
                    proc_params.append(param_info)
                
                proc_info = {
                    'name': proc_name,
                    'schema': schema_name,
                    'description': description,
                    'create_date': create_date.isoformat() if create_date else None,
                    'modify_date': modify_date.isoformat() if modify_date else None,
                    'parameters': proc_params,
                    'definition': proc_definition
                }
                
                self.schema_data['procedures'].append(proc_info)
            
        except Exception as e:
            print(f"Error extracting procedures from MSSQL: {str(e)}")

    def _extract_procedures_db2(self) -> None:
        """Extract stored procedure information from DB2"""
        try:
            # Get procedures
            query = """
                SELECT 
                    ROUTINESCHEMA, 
                    ROUTINENAME, 
                    CREATETIME, 
                    ALTERTIME, 
                    REMARKS,
                    ROUTINETYPE,
                    TEXT AS ROUTINE_DEFINITION
                FROM 
                    SYSCAT.ROUTINES
                WHERE 
                    ROUTINETYPE = 'P'
                    AND ROUTINESCHEMA NOT LIKE 'SYS%'
                    AND ROUTINESCHEMA NOT IN ('NULLID', 'SQLJ')
            """
            
            if self.schema:
                query += f" AND ROUTINESCHEMA = '{self.schema}'"
            
            query += " ORDER BY ROUTINESCHEMA, ROUTINENAME"
            
            procedures = self._execute_query(query)
            
            for proc in procedures:
                schema_name = proc.get('ROUTINESCHEMA', '').strip()
                proc_name = proc.get('ROUTINENAME', '').strip()
                
                # Get parameters for this procedure
                params_query = f"""
                    SELECT 
                        PARMNAME, 
                        TYPENAME, 
                        LENGTH, 
                        SCALE, 
                        ROWTYPE, 
                        ORDINAL,
                        REMARKS
                    FROM 
                        SYSCAT.ROUTINEPARMS
                    WHERE 
                        ROUTINESCHEMA = '{schema_name}'
                        AND ROUTINENAME = '{proc_name}'
                    ORDER BY 
                        ORDINAL
                """
                
                parameters = self._execute_query(params_query)
                
                proc_params = []
                
                for param in parameters:
                    param_name = param.get('PARMNAME', '').strip()
                    data_type = param.get('TYPENAME', '').strip()
                    length = param.get('LENGTH')
                    scale = param.get('SCALE')
                    row_type = param.get('ROWTYPE', '').strip()
                    ordinal = param.get('ORDINAL')
                    description = param.get('REMARKS', '').strip()
                    
                    # Format data type
                    formatted_data_type = data_type
                    if data_type in ('VARCHAR', 'CHAR', 'GRAPHIC', 'VARGRAPHIC'):
                        formatted_data_type += f'({length})'
                    elif data_type in ('DECIMAL'):
                        formatted_data_type += f'({length}, {scale})'
                    
                    param_info = {
                        'name': param_name,
                        'data_type': formatted_data_type,
                        'is_output': row_type == 'O' or row_type == 'B',
                        'has_default': False,  # DB2 doesn't expose this easily
                        'default_value': None,
                        'ordinal_position': ordinal,
                        'description': description
                    }
                    
                    proc_params.append(param_info)
                
                proc_info = {
                    'name': proc_name,
                    'schema': schema_name,
                    'description': proc.get('REMARKS', '').strip(),
                    'create_date': proc.get('CREATETIME', '').strip(),
                    'modify_date': proc.get('ALTERTIME', '').strip(),
                    'parameters': proc_params,
                    'definition': proc.get('ROUTINE_DEFINITION', '').strip()
                }
                
                self.schema_data['procedures'].append(proc_info)
            
        except Exception as e:
            print(f"Error extracting procedures from DB2: {str(e)}")
    
    def _extract_functions(self) -> None:
        """Extract function information"""
        if self.db_type == 'mssql':
            self._extract_functions_mssql()
        elif self.db_type == 'db2':
            self._extract_functions_db2()
    
    def _extract_functions_mssql(self) -> None:
        """Extract function information from MS SQL Server"""
        try:
            # Get functions
            self.cursor.execute("""
                SELECT 
                    f.name AS function_name,
                    s.name AS schema_name,
                    f.object_id,
                    f.create_date,
                    f.modify_date,
                    ISNULL(ep.value, '') AS description,
                    m.definition AS function_definition,
                    CASE 
                        WHEN f.type = 'IF' THEN 'Inline Table-valued Function'
                        WHEN f.type = 'TF' THEN 'Table-valued Function'
                        WHEN f.type = 'FN' THEN 'Scalar Function'
                        ELSE 'Unknown'
                    END AS function_type
                FROM 
                    sys.objects f
                INNER JOIN 
                    sys.schemas s ON f.schema_id = s.schema_id
                LEFT JOIN 
                    sys.extended_properties ep ON ep.major_id = f.object_id
                    AND ep.minor_id = 0
                    AND ep.class = 1
                    AND ep.name = 'MS_Description'
                LEFT JOIN 
                    sys.sql_modules m ON f.object_id = m.object_id
                WHERE 
                    f.type IN ('FN', 'IF', 'TF')
                    AND f.is_ms_shipped = 0
                ORDER BY 
                    s.name, f.name
            """)
            
            functions = self.cursor.fetchall()
            
            for func_name, schema_name, object_id, create_date, modify_date, description, func_definition, func_type in functions:
                # Get parameters for this function
                self.cursor.execute("""
                    SELECT 
                        p.name AS parameter_name,
                        t.name AS data_type,
                        p.max_length,
                        p.precision,
                        p.scale,
                        p.is_output,
                        p.has_default_value,
                        p.default_value,
                        p.parameter_id
                    FROM 
                        sys.parameters p
                    INNER JOIN 
                        sys.types t ON p.user_type_id = t.user_type_id
                    WHERE 
                        p.object_id = ?
                        AND p.parameter_id > 0  -- Skip return value
                    ORDER BY 
                        p.parameter_id
                """, (object_id,))
                
                parameters = self.cursor.fetchall()
                
                func_params = []
                
                for param_name, data_type, max_length, precision, scale, is_output, has_default, default_value, param_id in parameters:
                    # Format data type
                    formatted_data_type = data_type
                    if data_type in ('varchar', 'nvarchar', 'char', 'nchar'):
                        if max_length == -1:
                            formatted_data_type += '(MAX)'
                        else:
                            if data_type in ('nvarchar', 'nchar'):
                                max_length = max_length // 2
                            formatted_data_type += f'({max_length})'
                    elif data_type in ('decimal', 'numeric'):
                        formatted_data_type += f'({precision}, {scale})'
                    
                    param_info = {
                        'name': param_name,
                        'data_type': formatted_data_type,
                        'is_output': bool(is_output),
                        'has_default': bool(has_default),
                        'default_value': default_value,
                        'ordinal_position': param_id
                    }
                    
                    func_params.append(param_info)
                
                # Get return type
                return_type = "Unknown"
                if func_type == 'Scalar Function':
                    self.cursor.execute("""
                        SELECT 
                            t.name AS data_type,
                            p.max_length,
                            p.precision,
                            p.scale
                        FROM 
                            sys.parameters p
                        INNER JOIN 
                            sys.types t ON p.user_type_id = t.user_type_id
                        WHERE 
                            p.object_id = ?
                            AND p.parameter_id = 0  -- Return value
                    """, (object_id,))
                    
                    return_row = self.cursor.fetchone()
                    
                    if return_row:
                        data_type, max_length, precision, scale = return_row
                        return_type = data_type
                        
                        if data_type in ('varchar', 'nvarchar', 'char', 'nchar'):
                            if max_length == -1:
                                return_type += '(MAX)'
                            else:
                                if data_type in ('nvarchar', 'nchar'):
                                    max_length = max_length // 2
                                return_type += f'({max_length})'
                        elif data_type in ('decimal', 'numeric'):
                            return_type += f'({precision}, {scale})'
                
                func_info = {
                    'name': func_name,
                    'schema': schema_name,
                    'description': description,
                    'create_date': create_date.isoformat() if create_date else None,
                    'modify_date': modify_date.isoformat() if modify_date else None,
                    'parameters': func_params,
                    'definition': func_definition,
                    'type': func_type,
                    'return_type': return_type
                }
                
                self.schema_data['functions'].append(func_info)
            
        except Exception as e:
            print(f"Error extracting functions from MSSQL: {str(e)}")

    def _extract_functions_db2(self) -> None:
        """Extract function information from DB2"""
        try:
            # Get functions
            query = """
                SELECT 
                    ROUTINESCHEMA, 
                    ROUTINENAME, 
                    CREATETIME, 
                    ALTERTIME, 
                    REMARKS,
                    ROUTINETYPE,
                    TEXT AS ROUTINE_DEFINITION,
                    FUNCTIONTYPE,
                    RETURN_TYPENAME
                FROM 
                    SYSCAT.ROUTINES
                WHERE 
                    ROUTINETYPE = 'F'
                    AND ROUTINESCHEMA NOT LIKE 'SYS%'
                    AND ROUTINESCHEMA NOT IN ('NULLID', 'SQLJ')
            """
            
            if self.schema:
                query += f" AND ROUTINESCHEMA = '{self.schema}'"
            
            query += " ORDER BY ROUTINESCHEMA, ROUTINENAME"
            
            functions = self._execute_query(query)
            
            for func in functions:
                schema_name = func.get('ROUTINESCHEMA', '').strip()
                func_name = func.get('ROUTINENAME', '').strip()
                
                # Get parameters for this function
                params_query = f"""
                    SELECT 
                        PARMNAME, 
                        TYPENAME, 
                        LENGTH, 
                        SCALE, 
                        ROWTYPE, 
                        ORDINAL,
                        REMARKS
                    FROM 
                        SYSCAT.ROUTINEPARMS
                    WHERE 
                        ROUTINESCHEMA = '{schema_name}'
                        AND ROUTINENAME = '{func_name}'
                        AND ROWTYPE = 'P'  -- Input parameters only
                    ORDER BY 
                        ORDINAL
                """
                
                parameters = self._execute_query(params_query)
                
                func_params = []
                
                for param in parameters:
                    param_name = param.get('PARMNAME', '').strip()
                    data_type = param.get('TYPENAME', '').strip()
                    length = param.get('LENGTH')
                    scale = param.get('SCALE')
                    ordinal = param.get('ORDINAL')
                    description = param.get('REMARKS', '').strip()
                    
                    # Format data type
                    formatted_data_type = data_type
                    if data_type in ('VARCHAR', 'CHAR', 'GRAPHIC', 'VARGRAPHIC'):
                        formatted_data_type += f'({length})'
                    elif data_type in ('DECIMAL'):
                        formatted_data_type += f'({length}, {scale})'
                    
                    param_info = {
                        'name': param_name,
                        'data_type': formatted_data_type,
                        'is_output': False,
                        'has_default': False,  # DB2 doesn't expose this easily
                        'default_value': None,
                        'ordinal_position': ordinal,
                        'description': description
                    }
                    
                    func_params.append(param_info)
                
                # Determine function type
                function_type = func.get('FUNCTIONTYPE', '').strip()
                if function_type == 'C':
                    func_type = 'Column Function'
                elif function_type == 'R':
                    func_type = 'Row Function'
                elif function_type == 'S':
                    func_type = 'Scalar Function'
                elif function_type == 'T':
                    func_type = 'Table Function'
                else:
                    func_type = 'Unknown'
                
                func_info = {
                    'name': func_name,
                    'schema': schema_name,
                    'description': func.get('REMARKS', '').strip(),
                    'create_date': func.get('CREATETIME', '').strip(),
                    'modify_date': func.get('ALTERTIME', '').strip(),
                    'parameters': func_params,
                    'definition': func.get('ROUTINE_DEFINITION', '').strip(),
                    'type': func_type,
                    'return_type': func.get('RETURN_TYPENAME', '').strip()
                }
                
                self.schema_data['functions'].append(func_info)
            
        except Exception as e:
            print(f"Error extracting functions from DB2: {str(e)}")
    
    def _extract_indexes(self) -> None:
        """Extract index information"""
        if self.db_type == 'mssql':
            self._extract_indexes_mssql()
        elif self.db_type == 'db2':
            self._extract_indexes_db2()
    
    def _extract_indexes_mssql(self) -> None:
        """Extract index information from MS SQL Server"""
        try:
            # Get indexes
            self.cursor.execute("""
                SELECT 
                    i.name AS index_name,
                    OBJECT_NAME(i.object_id) AS table_name,
                    SCHEMA_NAME(o.schema_id) AS schema_name,
                    i.type_desc AS index_type,
                    i.is_unique,
                    i.is_primary_key,
                    i.is_unique_constraint
                FROM 
                    sys.indexes i
                INNER JOIN 
                    sys.objects o ON i.object_id = o.object_id
                WHERE 
                    i.name IS NOT NULL
                    AND o.type = 'U'  -- User tables only
                ORDER BY 
                    schema_name, table_name, index_name
            """)
            
            indexes = self.cursor.fetchall()

            for index_name, table_name, schema_name, index_type, is_unique, is_primary_key, is_unique_constraint in indexes:
                # Get columns for this index
                self.cursor.execute("""
                    SELECT 
                        COL_NAME(ic.object_id, ic.column_id) AS column_name,
                        ic.is_descending_key,
                        ic.is_included_column,
                        ic.key_ordinal
                    FROM 
                        sys.index_columns ic
                    WHERE 
                        ic.object_id = OBJECT_ID(?) 
                        AND ic.index_id = (
                            SELECT index_id FROM sys.indexes 
                            WHERE object_id = OBJECT_ID(?) AND name = ?
                        )
                    ORDER BY 
                        ic.key_ordinal
                """, (f'[{schema_name}].[{table_name}]', f'[{schema_name}].[{table_name}]', index_name))
                
                columns = self.cursor.fetchall()
                
                index_columns = []
                included_columns = []
                
                for column_name, is_descending, is_included, key_ordinal in columns:
                    if is_included:
                        included_columns.append(column_name)
                    else:
                        index_columns.append({
                            'name': column_name,
                            'is_descending': bool(is_descending),
                            'ordinal': key_ordinal
                        })
                
                index_info = {
                    'name': index_name,
                    'table': table_name,
                    'schema': schema_name,
                    'type': index_type,
                    'is_unique': bool(is_unique),
                    'is_primary_key': bool(is_primary_key),
                    'is_unique_constraint': bool(is_unique_constraint),
                    'columns': index_columns,
                    'included_columns': included_columns
                }
                
                self.schema_data['indexes'].append(index_info)
            
        except Exception as e:
            print(f"Error extracting indexes from MSSQL: {str(e)}")

    def _extract_indexes_db2(self) -> None:
        """Extract index information from DB2"""
        try:
            # Get indexes
            query = """
                SELECT 
                    INDNAME, 
                    TABSCHEMA, 
                    TABNAME, 
                    INDSCHEMA,
                    UNIQUERULE, 
                    INDEXTYPE,
                    CREATE_TIME,
                    REMARKS
                FROM 
                    SYSCAT.INDEXES
                WHERE 
                    TABSCHEMA NOT LIKE 'SYS%'
                    AND TABSCHEMA NOT IN ('NULLID', 'SQLJ')
            """
            
            if self.schema:
                query += f" AND TABSCHEMA = '{self.schema}'"
            
            query += " ORDER BY TABSCHEMA, TABNAME, INDNAME"
            
            indexes = self._execute_query(query)
            
            for idx in indexes:
                index_name = idx.get('INDNAME', '').strip()
                table_schema = idx.get('TABSCHEMA', '').strip()
                table_name = idx.get('TABNAME', '').strip()
                index_schema = idx.get('INDSCHEMA', '').strip()
                unique_rule = idx.get('UNIQUERULE', '').strip()
                index_type = idx.get('INDEXTYPE', '').strip()
                
                # Get columns for this index
                columns_query = f"""
                    SELECT 
                        COLNAME, 
                        COLORDER, 
                        COLSEQ
                    FROM 
                        SYSCAT.INDEXCOLUSE
                    WHERE 
                        INDSCHEMA = '{index_schema}'
                        AND INDNAME = '{index_name}'
                        AND TABSCHEMA = '{table_schema}'
                        AND TABNAME = '{table_name}'
                    ORDER BY 
                        COLSEQ
                """
                
                columns = self._execute_query(columns_query)
                
                index_columns = []
                
                for col in columns:
                    column_name = col.get('COLNAME', '').strip()
                    column_order = col.get('COLORDER', '').strip()
                    column_seq = col.get('COLSEQ')
                    
                    index_columns.append({
                        'name': column_name,
                        'is_descending': column_order == 'D',
                        'ordinal': column_seq
                    })
                
                index_info = {
                    'name': index_name,
                    'table': table_name,
                    'schema': table_schema,
                    'type': index_type,
                    'is_unique': unique_rule in ('P', 'U'),  # P=primary key, U=unique
                    'is_primary_key': unique_rule == 'P',
                    'is_unique_constraint': unique_rule == 'U',
                    'columns': index_columns,
                    'included_columns': [],  # DB2 doesn't have included columns in the same way
                    'description': idx.get('REMARKS', '').strip(),
                    'create_date': idx.get('CREATE_TIME', '').strip()
                }
                
                self.schema_data['indexes'].append(index_info)
            
        except Exception as e:
            print(f"Error extracting indexes from DB2: {str(e)}")

    def generate_documentation(self, output_format: str = 'json', output_file: str = None) -> Union[str, Dict]:
        """
        Generate database documentation in the specified format
        
        Args:
            output_format: Format of the output ('json', 'markdown', 'html')
            output_file: Path to the output file (if None, returns the content as string)
            
        Returns:
            Documentation content as string or dict
        """
        if not self.connection_successful:
            raise ConnectionError("Not connected to database")
        
        # Extract schema if not already done
        if not self.schema_data['tables']:
            self._extract_schema()
        
        if output_format.lower() == 'json':
            return self._generate_json_documentation(output_file)
        elif output_format.lower() == 'markdown':
            return self._generate_markdown_documentation(output_file)
        elif output_format.lower() == 'html':
            return self._generate_html_documentation(output_file)
        else:
            raise ValueError(f"Unsupported output format: {output_format}")
    
    def _generate_json_documentation(self, output_file: str = None) -> Union[str, Dict]:
        """Generate JSON documentation"""
        import json
        
        json_content = json.dumps(self.schema_data, indent=2)
        
        if output_file:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(json_content)
            return f"Documentation saved to {output_file}"
        else:
            return json_content
    
    def _generate_markdown_documentation(self, output_file: str = None) -> str:
        """Generate Markdown documentation"""
        md_content = f"# Database Documentation: {self.database}\n\n"
        
        # Add schemas
        if self.schema_data['schemas']:
            md_content += "## Schemas\n\n"
            for schema in self.schema_data['schemas']:
                md_content += f"### {schema['name']}\n\n"
                if schema.get('description'):
                    md_content += f"{schema['description']}\n\n"
                md_content += f"- Owner: {schema.get('owner', 'N/A')}\n"
                if schema.get('create_time'):
                    md_content += f"- Created: {schema.get('create_time')}\n"
                md_content += "\n"
        
        # Add tables
        if self.schema_data['tables']:
            md_content += "## Tables\n\n"
            for table in self.schema_data['tables']:
                md_content += f"### {table['schema']}.{table['name']}\n\n"
                if table.get('description'):
                    md_content += f"{table['description']}\n\n"
                
                md_content += f"- Created: {table.get('create_date', 'N/A')}\n"
                md_content += f"- Last Modified: {table.get('modify_date', 'N/A')}\n\n"
                
                # Add columns
                md_content += "#### Columns\n\n"
                md_content += "| Name | Data Type | Nullable | PK | Description |\n"
                md_content += "|------|-----------|----------|----|--------------|\n"
                
                for column in table.get('columns', []):
                    is_pk = "✓" if column['name'] in table.get('primary_keys', []) else ""
                    nullable = "Yes" if column.get('is_nullable') else "No"
                    description = column.get('description', '').replace("\n", "<br>")
                    
                    md_content += f"| {column['name']} | {column['data_type']} | {nullable} | {is_pk} | {description} |\n"
                
                md_content += "\n"
        
        # Add views
        if self.schema_data['views']:
            md_content += "## Views\n\n"
            for view in self.schema_data['views']:
                md_content += f"### {view['schema']}.{view['name']}\n\n"
                if view.get('description'):
                    md_content += f"{view['description']}\n\n"
                
                md_content += f"- Created: {view.get('create_date', 'N/A')}\n"
                md_content += f"- Last Modified: {view.get('modify_date', 'N/A')}\n\n"
                
                # Add columns
                md_content += "#### Columns\n\n"
                md_content += "| Name | Data Type | Nullable | Description |\n"
                md_content += "|------|-----------|----------|--------------|\n"
                
                for column in view.get('columns', []):
                    nullable = "Yes" if column.get('is_nullable') else "No"
                    description = column.get('description', '').replace("\n", "<br>")
                    
                    md_content += f"| {column['name']} | {column['data_type']} | {nullable} | {description} |\n"
                
                md_content += "\n"
                
                # Add definition
                if view.get('definition'):
                    md_content += "#### Definition\n\n"
                    md_content += f"```sql\n{view['definition']}\n```\n\n"
        
        # Add relationships
        if self.schema_data['relationships']:
            md_content += "## Relationships\n\n"
            md_content += "| Name | Source Table | Source Columns | Referenced Table | Referenced Columns | Delete Rule | Update Rule |\n"
            md_content += "|------|--------------|----------------|------------------|-------------------|-------------|-------------|\n"
            
            for rel in self.schema_data['relationships']:
                source_table = f"{rel['schema']}.{rel['table']}"
                source_columns = ", ".join(rel['columns'])
                ref_table = f"{rel['referenced_schema']}.{rel['referenced_table']}"
                ref_columns = ", ".join(rel['referenced_columns'])
                
                md_content += f"| {rel['name']} | {source_table} | {source_columns} | {ref_table} | {ref_columns} | {rel.get('delete_rule', 'N/A')} | {rel.get('update_rule', 'N/A')} |\n"
            
            md_content += "\n"
        
        # Add procedures
        if self.schema_data['procedures']:
            md_content += "## Stored Procedures\n\n"
            for proc in self.schema_data['procedures']:
                md_content += f"### {proc['schema']}.{proc['name']}\n\n"
                if proc.get('description'):
                    md_content += f"{proc['description']}\n\n"
                
                md_content += f"- Created: {proc.get('create_date', 'N/A')}\n"
                md_content += f"- Last Modified: {proc.get('modify_date', 'N/A')}\n\n"
                
                # Add parameters
                if proc.get('parameters'):
                    md_content += "#### Parameters\n\n"
                    md_content += "| Name | Data Type | Direction | Default | Description |\n"
                    md_content += "|------|-----------|-----------|---------|-------------|\n"
                    
                    for param in proc['parameters']:
                        direction = "OUT" if param.get('is_output') else "IN"
                        default = param.get('default_value', '') if param.get('has_default') else ''
                        description = param.get('description', '')
                        
                        md_content += f"| {param['name']} | {param['data_type']} | {direction} | {default} | {description} |\n"
                    
                    md_content += "\n"
                
                # Add definition
                if proc.get('definition'):
                    md_content += "#### Definition\n\n"
                    md_content += f"```sql\n{proc['definition']}\n```\n\n"
        
        # Add functions
        if self.schema_data['functions']:
            md_content += "## Functions\n\n"
            for func in self.schema_data['functions']:
                md_content += f"### {func['schema']}.{func['name']}\n\n"
                if func.get('description'):
                    md_content += f"{func['description']}\n\n"
                
                md_content += f"- Type: {func.get('type', 'N/A')}\n"
                md_content += f"- Return Type: {func.get('return_type', 'N/A')}\n"
                md_content += f"- Created: {func.get('create_date', 'N/A')}\n"
                md_content += f"- Last Modified: {func.get('modify_date', 'N/A')}\n\n"
                
                # Add parameters
                if func.get('parameters'):
                    md_content += "#### Parameters\n\n"
                    md_content += "| Name | Data Type | Default | Description |\n"
                    md_content += "|------|-----------|---------|-------------|\n"
                    
                    for param in func['parameters']:
                        default = param.get('default_value', '') if param.get('has_default') else ''
                        description = param.get('description', '')
                        
                        md_content += f"| {param['name']} | {param['data_type']} | {default} | {description} |\n"
                    
                    md_content += "\n"
                
                # Add definition
                if func.get('definition'):
                    md_content += "#### Definition\n\n"
                    md_content += f"```sql\n{func['definition']}\n```\n\n"
        
        # Add indexes
        if self.schema_data['indexes']:
            md_content += "## Indexes\n\n"
            md_content += "## Indexes\n\n"
            md_content += "| Name | Table | Type | Unique | Columns |\n"
            md_content += "|------|-------|------|--------|--------|\n"
            
            for idx in self.schema_data['indexes']:
                table_name = f"{idx['schema']}.{idx['table']}"
                unique = "Yes" if idx.get('is_unique') else "No"
                
                # Format columns
                columns = []
                for col in idx.get('columns', []):
                    direction = "DESC" if col.get('is_descending') else "ASC"
                    columns.append(f"{col['name']} {direction}")
                
                # Add included columns if any
                if idx.get('included_columns'):
                    columns.append(f"INCLUDE: {', '.join(idx['included_columns'])}")
                
                columns_str = ", ".join(columns)
                
                md_content += f"| {idx['name']} | {table_name} | {idx.get('type', 'N/A')} | {unique} | {columns_str} |\n"
            
            md_content += "\n"
        
        if output_file:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(md_content)
            return f"Documentation saved to {output_file}"
        else:
            return md_content
    
    def _generate_html_documentation(self, output_file: str = None) -> str:
        """Generate HTML documentation"""
        # Get markdown content first
        md_content = self._generate_markdown_documentation()
        
        # Convert markdown to HTML
        try:
            import markdown
            html_content = markdown.markdown(md_content, extensions=['tables', 'fenced_code'])
        except ImportError:
            # Fallback to a simple HTML conversion if markdown module is not available
            html_content = f"<pre>{md_content}</pre>"
            print("Warning: 'markdown' module not found. Using simple HTML conversion.")
        
        # Wrap in a basic HTML template
        html_doc = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Database Documentation: {self.database}</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            line-height: 1.6;
            margin: 0;
            padding: 20px;
            color: #333;
        }}
        h1, h2, h3, h4 {{
            color: #2c3e50;
        }}
        h1 {{
            border-bottom: 2px solid #eee;
            padding-bottom: 10px;
        }}
        h2 {{
            border-bottom: 1px solid #eee;
            padding-bottom: 5px;
            margin-top: 30px;
        }}
        h3 {{
            margin-top: 25px;
        }}
        table {{
            border-collapse: collapse;
            width: 100%;
            margin: 20px 0;
        }}
        th, td {{
            border: 1px solid #ddd;
            padding: 8px;
            text-align: left;
        }}
        th {{
            background-color: #f2f2f2;
        }}
        tr:nth-child(even) {{
            background-color: #f9f9f9;
        }}
        pre {{
            background-color: #f5f5f5;
            padding: 15px;
            border-radius: 5px;
            overflow-x: auto;
        }}
        code {{
            font-family: Consolas, Monaco, 'Andale Mono', monospace;
            background-color: #f5f5f5;
            padding: 2px 4px;
            border-radius: 3px;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
        }}
        .toc {{
            background-color: #f8f9fa;
            border: 1px solid #eaecef;
            border-radius: 3px;
            padding: 15px;
            margin-bottom: 20px;
        }}
        .toc ul {{
            list-style-type: none;
            padding-left: 20px;
        }}
        .toc li {{
            margin: 5px 0;
        }}
    </style>
</head>
<body>
    <div class="container">
        {html_content}
    </div>
</body>
</html>
"""
        
        if output_file:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(html_doc)
            return f"Documentation saved to {output_file}"
        else:
            return html_doc

def generate_markdown(self, output_dir: str) -> str:
    """Generate Markdown documentation"""
    if not HAS_PYODBC:
        raise ImportError("pyodbc is not installed. Install with 'pip install pyodbc'")
        
    os.makedirs(output_dir, exist_ok=True)
        
    # Create main markdown file
    main_file = os.path.join(output_dir, f"{self.schema_data['database']}_documentation.md")
        
    with open(main_file, 'w', encoding='utf-8') as f:
        # Write header
        f.write(f"# Database Documentation: {self.schema_data['database']}\n\n")
        f.write(f"**Server:** {self.schema_data['server']}\n")
        f.write(f"**Generated:** {self.schema_data['extracted_date']}\n\n")
                
        # Write table of contents
        f.write("## Table of Contents\n\n")
        f.write("1. [Schemas](#schemas)\n")
        f.write("2. [Tables](#tables)\n")
        f.write("3. [Views](#views)\n")
        f.write("4. [Relationships](#relationships)\n")
        f.write("5. [Stored Procedures](#stored-procedures)\n")
        f.write("6. [Functions](#functions)\n")
        f.write("7. [Indexes](#indexes)\n\n")
                
        # Write schemas section
        f.write("## Schemas\n\n")
        if self.schema_data['schemas']:
            f.write("| Schema | Owner | Description |\n")
            f.write("|--------|-------|-------------|\n")
            for schema in self.schema_data['schemas']:
                f.write(f"| {schema['name']} | {schema['owner']} | {schema['description']} |\n")
        else:
            f.write("No schemas found.\n")
        f.write("\n")
                
        # Write tables section
        f.write("## Tables\n\n")
        if self.schema_data['tables']:
            for table in self.schema_data['tables']:
                f.write(f"### {table['full_name']}\n\n")
                if table['description']:
                    f.write(f"**Description:** {table['description']}\n\n")
                                
                f.write("**Columns:**\n\n")
                f.write("| Column | Type | Nullable | Default | PK | FK | Description |\n")
                f.write("|--------|------|----------|---------|----|----|-------------|\n")
                                
                for column in table['columns']:
                    pk = "✓" if column.get('primary_key') else ""
                    fk = "✓" if column.get('foreign_key') else ""
                    nullable = "YES" if column.get('nullable') else "NO"
                    default = column.get('default', '')
                    description = column.get('description', '')
                                        
                    f.write(f"| {column['name']} | {column['type']} | {nullable} | {default} | {pk} | {fk} | {description} |\n")
                                
                f.write("\n")
        else:
            f.write("No tables found.\n")
        f.write("\n")
                
        # Write views section
        f.write("## Views\n\n")
        if self.schema_data['views']:
            for view in self.schema_data['views']:
                f.write(f"### {view['full_name']}\n\n")
                if view['description']:
                    f.write(f"**Description:** {view['description']}\n\n")
                                
                f.write("**Columns:**\n\n")
                f.write("| Column | Type | Nullable | Description |\n")
                f.write("|--------|------|----------|-------------|\n")
                                
                for column in view['columns']:
                    nullable = "YES" if column.get('nullable') else "NO"
                    description = column.get('description', '')
                                        
                    f.write(f"| {column['name']} | {column['type']} | {nullable} | {description} |\n")
                                
                f.write("\n**Definition:**\n\n")
                f.write("```sql\n")
                f.write(view['definition'] if view['definition'] else "-- Definition not available")
                f.write("\n```\n\n")
        else:
            f.write("No views found.\n")
        f.write("\n")
                
        # Write relationships section
        f.write("## Relationships\n\n")
        if self.schema_data['relationships']:
            f.write("| Constraint | Table | Column | Referenced Table | Referenced Column | Delete Rule | Update Rule |\n")
            f.write("|------------|-------|--------|------------------|-------------------|-------------|-------------|\n")
                        
            for rel in self.schema_data['relationships']:
                f.write(f"| {rel['name']} | {rel['schema']}.{rel['table']} | {', '.join(rel['columns'])} | ")
                f.write(f"{rel['referenced_schema']}.{rel['referenced_table']} | {', '.join(rel['referenced_columns'])} | ")
                f.write(f"{rel['delete_rule']} | {rel['update_rule']} |\n")
        else:
            f.write("No relationships found.\n")
        f.write("\n")
                
        # Write stored procedures section
        f.write("## Stored Procedures\n\n")
        if self.schema_data['procedures']:
            for proc in self.schema_data['procedures']:
                f.write(f"### {proc['full_name']}\n\n")
                if proc['description']:
                    f.write(f"**Description:** {proc['description']}\n\n")
                                
                if proc['parameters']:
                    f.write("**Parameters:**\n\n")
                    f.write("| Parameter | Type | Direction | Default |\n")
                    f.write("|-----------|------|-----------|--------|\n")
                                        
                    for param in proc['parameters']:
                        direction = "OUT" if param.get('is_output') else "IN"
                        default = param.get('default_value', '') if param.get('has_default') else ''
                                                
                        f.write(f"| {param['name']} | {param['type']} | {direction} | {default} |\n")
                                        
                    f.write("\n")
                                
                f.write("**Definition:**\n\n")
                f.write("```sql\n")
                f.write(proc['definition'] if proc['definition'] else "-- Definition not available")
                f.write("\n```\n\n")
        else:
            f.write("No stored procedures found.\n")
        f.write("\n")
                
        # Write functions section
        f.write("## Functions\n\n")
        if self.schema_data['functions']:
            for func in self.schema_data['functions']:
                f.write(f"### {func['full_name']}\n\n")
                f.write(f"**Type:** {func['type']}\n\n")
                                
                if func['description']:
                    f.write(f"**Description:** {func['description']}\n\n")
                                
                if 'return_type' in func:
                    # Check if return_type is a dictionary or a string
                    if isinstance(func['return_type'], dict):
                        f.write(f"**Returns:** {func['return_type'].get('type', 'Unknown')}\n\n")
                    else:
                        f.write(f"**Returns:** {func['return_type']}\n\n")
                                
                if func['parameters']:
                    f.write("**Parameters:**\n\n")
                    f.write("| Parameter | Type | Default |\n")
                    f.write("|-----------|------|--------|\n")
                                        
                    for param in func['parameters']:
                        default = param.get('default_value', '') if param.get('has_default') else ''
                                                
                        f.write(f"| {param['name']} | {param['type']} | {default} |\n")
                                        
                    f.write("\n")
                                
                f.write("**Definition:**\n\n")
                f.write("```sql\n")
                f.write(func['definition'] if func['definition'] else "-- Definition not available")
                f.write("\n```\n\n")
        else:
            f.write("No functions found.\n")
        f.write("\n")
                
        # Write indexes section
        f.write("## Indexes\n\n")
        if self.schema_data['indexes']:
            f.write("| Index | Table | Type | Unique | Primary Key | Columns |\n")
            f.write("|-------|-------|------|--------|-------------|--------|\n")
                        
            for idx in self.schema_data['indexes']:
                unique = "✓" if idx.get('is_unique') else ""
                pk = "✓" if idx.get('is_primary_key') else ""
                
                # Format columns as a string if it's a list
                columns = idx['columns']
                if isinstance(columns, list):
                    if all(isinstance(col, dict) for col in columns):
                        # If columns is a list of dictionaries
                        formatted_columns = ", ".join([f"{col['name']} {'DESC' if col.get('is_descending') else 'ASC'}" for col in columns])
                    else:
                        # If columns is a simple list of strings
                        formatted_columns = ", ".join(columns)
                else:
                    # If columns is already a string
                    formatted_columns = columns
                                
                f.write(f"| {idx['name']} | {idx['schema']}.{idx['table']} | {idx['type']} | {unique} | {pk} | {formatted_columns} |\n")
        else:
            f.write("No indexes found.\n")
            
    print(f"Markdown documentation generated: {main_file}")
    return main_file


    def generate_excel(self, output_dir: str) -> str:
        """Generate Excel documentation"""
        if not EXCEL_AVAILABLE:
            raise ImportError("pandas and openpyxl are not installed. Install with 'pip install pandas openpyxl'")
        
        os.makedirs(output_dir, exist_ok=True)
        
        # Create Excel file
        excel_file = os.path.join(output_dir, f"{self.schema_data['database']}_documentation.xlsx")
        
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            # Create Overview sheet
            overview_data = {
                'Property': ['Database', 'Server', 'Generated Date'],
                'Value': [
                    self.schema_data['database'],
                    self.schema_data['server'],
                    self.schema_data['extracted_date']
                ]
            }
            pd.DataFrame(overview_data).to_excel(writer, sheet_name='Overview', index=False)
            
            # Create Schemas sheet
            if self.schema_data['schemas']:
                schemas_df = pd.DataFrame(self.schema_data['schemas'])
                schemas_df.to_excel(writer, sheet_name='Schemas', index=False)
            
            # Create Tables sheet
            if self.schema_data['tables']:
                # Flatten table data for Excel
                tables_data = []
                for table in self.schema_data['tables']:
                    for column in table['columns']:
                        tables_data.append({
                            'Schema': table['schema'],
                            'Table': table['name'],
                            'Table Description': table['description'],
                            'Column': column['name'],
                            'Type': column['type'],
                            'Nullable': column.get('nullable', False),
                            'Default': column.get('default', ''),
                            'Primary Key': column.get('primary_key', False),
                            'Foreign Key': column.get('foreign_key', False),
                            'Identity': column.get('identity', False),
                            'Description': column.get('description', '')
                        })
                
                if tables_data:
                    tables_df = pd.DataFrame(tables_data)
                    tables_df.to_excel(writer, sheet_name='Tables', index=False)
            
            # Create Views sheet
            if self.schema_data['views']:
                # Flatten view data for Excel
                views_data = []
                for view in self.schema_data['views']:
                    for column in view['columns']:
                        views_data.append({
                            'Schema': view['schema'],
                            'View': view['name'],
                            'View Description': view['description'],
                            'Column': column['name'],
                            'Type': column['type'],
                            'Nullable': column.get('nullable', False),
                            'Description': column.get('description', '')
                        })
                
                if views_data:
                    views_df = pd.DataFrame(views_data)
                    views_df.to_excel(writer, sheet_name='Views', index=False)
            
            # Create Relationships sheet
            if self.schema_data['relationships']:
                relationships_df = pd.DataFrame(self.schema_data['relationships'])
                relationships_df.to_excel(writer, sheet_name='Relationships', index=False)
            
            # Create Procedures sheet
            if self.schema_data['procedures']:
                # Flatten procedure data for Excel
                procedures_data = []
                for proc in self.schema_data['procedures']:
                    proc_row = {
                        'Schema': proc['schema'],
                        'Procedure': proc['name'],
                        'Description': proc['description'],
                        'Created': proc.get('created', ''),
                        'Modified': proc.get('modified', '')
                    }
                    procedures_data.append(proc_row)
                
                if procedures_data:
                    procedures_df = pd.DataFrame(procedures_data)
                    procedures_df.to_excel(writer, sheet_name='Procedures', index=False)
                
                # Create Procedure Parameters sheet
                proc_params_data = []
                for proc in self.schema_data['procedures']:
                    for param in proc['parameters']:
                        proc_params_data.append({
                            'Schema': proc['schema'],
                            'Procedure': proc['name'],
                            'Parameter': param['name'],
                            'Type': param['type'],
                            'Direction': 'OUT' if param.get('is_output') else 'IN',
                            'Default': param.get('default_value', '') if param.get('has_default') else ''
                        })
                
                if proc_params_data:
                    proc_params_df = pd.DataFrame(proc_params_data)
                    proc_params_df.to_excel(writer, sheet_name='Procedure Parameters', index=False)
            
            # Create Functions sheet
            if self.schema_data['functions']:
                # Flatten function data for Excel
                functions_data = []
                for func in self.schema_data['functions']:
                    func_row = {
                        'Schema': func['schema'],
                        'Function': func['name'],
                        'Type': func['type'],
                        'Return Type': func.get('return_type', {}).get('type', ''),
                        'Description': func['description'],
                        'Created': func.get('created', ''),
                        'Modified': func.get('modified', '')
                    }
                    functions_data.append(func_row)
                
                if functions_data:
                    functions_df = pd.DataFrame(functions_data)
                    functions_df.to_excel(writer, sheet_name='Functions', index=False)
                
                # Create Function Parameters sheet
                func_params_data = []
                for func in self.schema_data['functions']:
                    for param in func['parameters']:
                        func_params_data.append({
                            'Schema': func['schema'],
                            'Function': func['name'],
                            'Parameter': param['name'],
                            'Type': param['type'],
                            'Default': param.get('default_value', '') if param.get('has_default') else ''
                        })
                
                if func_params_data:
                    func_params_df = pd.DataFrame(func_params_data)
                    func_params_df.to_excel(writer, sheet_name='Function Parameters', index=False)
            
            # Create Indexes sheet
            if self.schema_data['indexes']:
                indexes_df = pd.DataFrame(self.schema_data['indexes'])
                indexes_df.to_excel(writer, sheet_name='Indexes', index=False)
        
        print(f"Excel documentation generated: {excel_file}")
        return excel_file
    def generate_csv(self, output_dir: str) -> List[str]:
        """Generate CSV documentation"""
        os.makedirs(output_dir, exist_ok=True)
        
        generated_files = []
        
        # Create Overview CSV
        overview_file = os.path.join(output_dir, f"{self.schema_data['database']}_overview.csv")
        with open(overview_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Property', 'Value'])
            writer.writerow(['Database', self.schema_data['database']])
            writer.writerow(['Server', self.schema_data['server']])
            writer.writerow(['Generated Date', self.schema_data['extracted_date']])
            
        generated_files.append(overview_file)
        
        # Create Schemas CSV
        if self.schema_data['schemas']:
            schemas_file = os.path.join(output_dir, f"{self.schema_data['database']}_schemas.csv")
            with open(schemas_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                # Get all possible keys
                fieldnames = set()
                for schema in self.schema_data['schemas']:
                    fieldnames.update(schema.keys())
                    
                writer.writerow(fieldnames)
                for schema in self.schema_data['schemas']:
                    writer.writerow([schema.get(field, '') for field in fieldnames])
                    
            generated_files.append(schemas_file)
        
        # Create Tables CSV
        if self.schema_data['tables']:
            tables_file = os.path.join(output_dir, f"{self.schema_data['database']}_tables.csv")
            with open(tables_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'Schema', 'Table', 'Table Description', 'Column', 'Type', 
                    'Nullable', 'Default', 'Primary Key', 'Foreign Key',
                    'Identity', 'Description'
                ])
                
                for table in self.schema_data['tables']:
                    for column in table['columns']:
                        writer.writerow([
                            table['schema'],
                            table['name'],
                            table['description'],
                            column['name'],
                            column['type'],
                            column.get('nullable', False),
                            column.get('default', ''),
                            column.get('primary_key', False),
                            column.get('foreign_key', False),
                            column.get('identity', False),
                            column.get('description', '')
                        ])
            
            generated_files.append(tables_file)

            
            # Create Views CSV
            if self.schema_data['views']:
                views_file = os.path.join(output_dir, f"{self.schema_data['database']}_views.csv")
                with open(views_file, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        'Schema', 'View', 'View Description', 'Column', 'Type', 
                        'Nullable', 'Description'
                    ])
                    
                    for view in self.schema_data['views']:
                        for column in view['columns']:
                            writer.writerow([
                                view['schema'],
                                view['name'],
                                view['description'],
                                column['name'],
                                column['type'],
                                column.get('nullable', False),
                                column.get('description', '')
                            ])
                
                generated_files.append(views_file)
            
            # Create Relationships CSV
            if self.schema_data['relationships']:
                relationships_file = os.path.join(output_dir, f"{self.schema_data['database']}_relationships.csv")
                with open(relationships_file, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    # Get all possible keys
                    fieldnames = set()
                    for rel in self.schema_data['relationships']:
                        fieldnames.update(rel.keys())
                    
                    writer.writerow(fieldnames)
                    for rel in self.schema_data['relationships']:
                        writer.writerow([rel.get(field, '') for field in fieldnames])
                
                generated_files.append(relationships_file)
            
            # Create Procedures CSV
            if self.schema_data['procedures']:
                procedures_file = os.path.join(output_dir, f"{self.schema_data['database']}_procedures.csv")
                with open(procedures_file, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        'Schema', 'Procedure', 'Description', 'Created', 'Modified'
                    ])
                    
                    for proc in self.schema_data['procedures']:
                        writer.writerow([
                            proc['schema'],
                            proc['name'],
                            proc['description'],
                            proc.get('created', ''),
                            proc.get('modified', '')
                        ])
                
                generated_files.append(procedures_file)
                
                # Create Procedure Parameters CSV
                proc_params_file = os.path.join(output_dir, f"{self.schema_data['database']}_procedure_parameters.csv")
                with open(proc_params_file, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        'Schema', 'Procedure', 'Parameter', 'Type', 'Direction', 'Default'
                    ])
                    
                    for proc in self.schema_data['procedures']:
                        for param in proc['parameters']:
                            writer.writerow([
                                proc['schema'],
                                proc['name'],
                                param['name'],
                                param['type'],
                                'OUT' if param.get('is_output') else 'IN',
                                param.get('default_value', '') if param.get('has_default') else ''
                            ])
                
                generated_files.append(proc_params_file)
            
            # Create Functions CSV
            if self.schema_data['functions']:
                functions_file = os.path.join(output_dir, f"{self.schema_data['database']}_functions.csv")
                with open(functions_file, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        'Schema', 'Function', 'Type', 'Return Type', 'Description', 'Created', 'Modified'
                    ])
                    
                    for func in self.schema_data['functions']:
                        writer.writerow([
                            func['schema'],
                            func['name'],
                            func['type'],
                            func.get('return_type', {}).get('type', ''),
                            func['description'],
                            func.get('created', ''),
                            func.get('modified', '')
                        ])
                
                generated_files.append(functions_file)
                
                # Create Function Parameters CSV
                func_params_file = os.path.join(output_dir, f"{self.schema_data['database']}_function_parameters.csv")
                with open(func_params_file, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        'Schema', 'Function', 'Parameter', 'Type', 'Default'
                    ])
                    
                    for func in self.schema_data['functions']:
                        for param in func['parameters']:
                            writer.writerow([
                                func['schema'],
                                func['name'],
                                param['name'],
                                param['type'],
                                param.get('default_value', '') if param.get('has_default') else ''
                            ])
                
                generated_files.append(func_params_file)
            
            # Create Indexes CSV
            if self.schema_data['indexes']:
                indexes_file = os.path.join(output_dir, f"{self.schema_data['database']}_indexes.csv")
                with open(indexes_file, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    # Get all possible keys
                    fieldnames = set()
                    for idx in self.schema_data['indexes']:
                        fieldnames.update(idx.keys())
                    
                    writer.writerow(fieldnames)
                    for idx in self.schema_data['indexes']:
                        writer.writerow([idx.get(field, '') for field in fieldnames])
                
                generated_files.append(indexes_file)
            
            print(f"CSV documentation generated in: {output_dir}")
            return generated_files
    
    def generate_json(self, output_dir: str) -> str:
        """Generate JSON documentation"""
        os.makedirs(output_dir, exist_ok=True)
        
        # Create JSON file
        json_file = os.path.join(output_dir, f"{self.schema_data['database']}_documentation.json")
        
        with open(json_file, 'w', encoding='utf-8') as f:
            # Convert datetime objects to strings for JSON serialization
            def json_serial(obj):
                if isinstance(obj, (datetime)):
                    return obj.isoformat()
                raise TypeError(f"Type {type(obj)} not serializable")
            
            json.dump(self.schema_data, f, default=json_serial, indent=2)
        
        print(f"JSON documentation generated: {json_file}")
        return json_file
    
    def generate_erd(self, output_dir: str) -> str:
        """Generate an Entity Relationship Diagram using Graphviz"""
        try:
            import graphviz
        except ImportError:
            print("Error: graphviz Python package is not installed. Install it with 'pip install graphviz'")
            return None
        
        # Check if Graphviz executable is available
        if hasattr(self, 'graphviz_path') and self.graphviz_path:
            # Add Graphviz to PATH temporarily
            os.environ["PATH"] = self.graphviz_path + os.pathsep + os.environ["PATH"]
        
        try:
            # Create output directory if it doesn't exist
            os.makedirs(output_dir, exist_ok=True)
            
            # Create a new graph
            dot = graphviz.Digraph(
                name=f"{self.schema_data['database']}_ERD",
                comment=f"ERD for {self.schema_data['database']}",
                format='svg',
                engine='dot'
            )
            
            # Set graph attributes
            dot.attr('graph', 
                     rankdir='LR',
                     splines='ortho',
                     nodesep='0.8',
                     ranksep='1.0',
                     fontname='Arial',
                     fontsize='12')
            
            dot.attr('node', 
                     shape='record',
                     fontname='Arial',
                     fontsize='10',
                     margin='0.1,0.1')
            
            dot.attr('edge', 
                     fontname='Arial',
                     fontsize='8')
            
            # Add tables as nodes
            for table in self.schema_data['tables']:
                table_name = table['name']
                
                # Create label with table name and columns
                label = f"{{<table>{table_name}|"
                
                # Add primary key columns first
                pk_columns = [col for col in table['columns'] if col.get('is_primary_key', False)]
                for col in pk_columns:
                    data_type = col.get('data_type', '')
                    label += f"<{col['name']}> {col['name']} ({data_type}) PK\\l"
                
                # Add foreign key columns
                fk_columns = [col for col in table['columns'] if col.get('is_foreign_key', False) and not col.get('is_primary_key', False)]
                for col in fk_columns:
                    data_type = col.get('data_type', '')
                    label += f"<{col['name']}> {col['name']} ({data_type}) FK\\l"
                
                # Add other columns
                other_columns = [col for col in table['columns'] 
                                if not col.get('is_primary_key', False) and not col.get('is_foreign_key', False)]
                for col in other_columns:
                    data_type = col.get('data_type', '')
                    label += f"<{col['name']}> {col['name']} ({data_type})\\l"
                
                label += "}"
                
                dot.node(table_name, label=label)
            
            # Add relationships as edges
            for table in self.schema_data['tables']:
                for column in table['columns']:
                    if column.get('is_foreign_key', False) and 'references' in column:
                        ref = column['references']
                        if isinstance(ref, dict) and 'table' in ref and 'column' in ref:
                            source_table = table['name']
                            target_table = ref['table']
                            source_col = column['name']
                            target_col = ref['column']
                            
                            # Create edge with labels
                            dot.edge(f"{source_table}:{source_col}", 
                                    f"{target_table}:{target_col}",
                                    headlabel=f" {target_col} ",
                                    taillabel=f" {source_col} ")
            
            # Save the diagram
            output_file = os.path.join(output_dir, f"{self.schema_data['database']}_ERD")
            try:
                # Try to render the diagram
                dot.render(output_file, cleanup=True)
                print(f"ERD diagram generated: {output_file}.svg")
                return f"{output_file}.svg"
            except Exception as e:
                # If rendering fails, try to save the DOT file at least
                dot_file = f"{output_file}.dot"
                with open(dot_file, 'w', encoding='utf-8') as f:
                    f.write(dot.source)
                print(f"Error rendering ERD diagram: {e}")
                print(f"DOT file saved: {dot_file}")
                print("You can manually render it using Graphviz: dot -Tsvg -o output.svg input.dot")
                return None
        except Exception as e:
            print(f"Error generating ERD diagram: {e}")
            import traceback
            traceback.print_exc()
            return None

  
    def generate_html(self, output_dir: str) -> str:
        """Generate HTML documentation"""
        os.makedirs(output_dir, exist_ok=True)
        
        # Create HTML file
        html_file = os.path.join(output_dir, f"{self.schema_data['database']}_documentation.html")
        
        with open(html_file, 'w', encoding='utf-8') as f:
            # Write HTML header
            f.write(f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Database Documentation: {self.schema_data['database']}</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            line-height: 1.6;
            margin: 0;
            padding: 20px;
            color: #333;
        }}
        h1, h2, h3, h4 {{
            color: #2c3e50;
        }}
        table {{
            border-collapse: collapse;
            width: 100%;
            margin-bottom: 20px;
        }}
        th, td {{
            border: 1px solid #ddd;
            padding: 8px;
            text-align: left;
        }}
        th {{
            background-color: #f2f2f2;
            font-weight: bold;
        }}
        tr:nth-child(even) {{
            background-color: #f9f9f9;
        }}
        .nav {{
            background-color: #f8f9fa;
            padding: 10px;
            margin-bottom: 20px;
            border-radius: 5px;
        }}
        .nav a {{
            margin-right: 15px;
            text-decoration: none;
            color: #007bff;
        }}
        .nav a:hover {{
            text-decoration: underline;
        }}
        .section {{
            margin-bottom: 30px;
        }}
        .subsection {{
            margin-bottom: 20px;
            padding: 10px;
            background-color: #f8f9fa;
            border-radius: 5px;
        }}
        .description {{
            font-style: italic;
            color: #666;
        }}
        pre {{
            background-color: #f5f5f5;
            padding: 10px;
            border-radius: 5px;
            overflow-x: auto;
        }}
        .pk {{
            color: #d35400;
            font-weight: bold;
        }}
        .fk {{
            color: #2980b9;
            font-weight: bold;
        }}
        .nullable {{
            color: #7f8c8d;
        }}
        .not-nullable {{
            color: #2c3e50;
            font-weight: bold;
        }}
    </style>
</head>
<body>
    <h1>Database Documentation: {self.schema_data['database']}</h1>
    <p><strong>Server:</strong> {self.schema_data['server']}</p>
    <p><strong>Generated:</strong> {self.schema_data['extracted_date']}</p>
    
    <div class="nav">
        <a href="#schemas">Schemas</a>
        <a href="#tables">Tables</a>
        <a href="#views">Views</a>
        <a href="#relationships">Relationships</a>
        <a href="#procedures">Stored Procedures</a>
        <a href="#functions">Functions</a>
        <a href="#indexes">Indexes</a>
    </div>
""")
            
            # Write schemas section
            f.write("""
    <div class="section" id="schemas">
        <h2>Schemas</h2>
""")
            if self.schema_data['schemas']:
                f.write("""
        <table>
            <tr>
                <th>Schema</th>
                <th>Owner</th>
                <th>Description</th>
            </tr>
""")
                for schema in self.schema_data['schemas']:
                    f.write(f"""
            <tr>
                <td>{schema['name']}</td>
                <td>{schema['owner']}</td>
                <td>{schema['description']}</td>
            </tr>
""")
                f.write("""
        </table>
""")
            else:
                f.write("<p>No schemas found.</p>")
            f.write("</div>")
            
            # Write tables section
            f.write("""
    <div class="section" id="tables">
        <h2>Tables</h2>
""")
            if self.schema_data['tables']:
                for table in self.schema_data['tables']:
                    f.write(f"""
        <div class="subsection">
            <h3>{table['full_name']}</h3>
""")
                    if table['description']:
                        f.write(f"""
            <p class="description">{table['description']}</p>
""")
                    f.write("""
            <h4>Columns</h4>
            <table>
                <tr>
                    <th>Column</th>
                    <th>Type</th>
                    <th>Nullable</th>
                    <th>Default</th>
                    <th>PK</th>
                    <th>FK</th>
                    <th>Description</th>
                </tr>
""")
                    for column in table['columns']:
                        pk = "✓" if column.get('primary_key') else ""
                        fk = "✓" if column.get('foreign_key') else ""
                        nullable = "YES" if column.get('nullable') else "NO"
                        default = column.get('default', '')
                        description = column.get('description', '')
                        
                        # Add CSS classes for styling
                        col_class = ""
                        if column.get('primary_key'):
                            col_class += " pk"
                        if column.get('foreign_key'):
                            col_class += " fk"
                        
                        nullable_class = "nullable" if column.get('nullable') else "not-nullable"
                        
                        f.write(f"""
                <tr>
                    <td class="{col_class}">{column['name']}</td>
                    <td>{column['type']}</td>
                    <td class="{nullable_class}">{nullable}</td>
                    <td>{default}</td>
                    <td>{pk}</td>
                    <td>{fk}</td>
                    <td>{description}</td>
                </tr>
""")
                    f.write("""
            </table>
        </div>
""")
            else:
                f.write("<p>No tables found.</p>")
            f.write("</div>")
            
            # Write views section
            f.write("""
    <div class="section" id="views">
        <h2>Views</h2>
""")
            if self.schema_data['views']:
                for view in self.schema_data['views']:
                    f.write(f"""
        <div class="subsection">
            <h3>{view['full_name']}</h3>
""")
                    if view['description']:
                        f.write(f"""
            <p class="description">{view['description']}</p>
""")
                    f.write("""
            <h4>Columns</h4>
            <table>
                <tr>
                    <th>Column</th>
                    <th>Type</th>
                    <th>Nullable</th>
                    <th>Description</th>
                </tr>
""")
                    for column in view['columns']:
                        nullable = "YES" if column.get('nullable') else "NO"
                        description = column.get('description', '')
                        
                        nullable_class = "nullable" if column.get('nullable') else "not-nullable"
                        
                        f.write(f"""
                <tr>
                    <td>{column['name']}</td>
                    <td>{column['type']}</td>
                    <td class="{nullable_class}">{nullable}</td>
                    <td>{description}</td>
                </tr>
""")
                    f.write("""
            </table>
            
            <h4>Definition</h4>
            <pre>{}</pre>
        </div>
""".format(view['definition'] if view['definition'] else "-- Definition not available"))
            else:
                f.write("<p>No views found.</p>")
            f.write("</div>")
            
            # Write relationships section
            f.write("""
    <div class="section" id="relationships">
        <h2>Relationships</h2>
""")
            if self.schema_data['relationships']:
                f.write("""
        <table>
            <tr>
                <th>Constraint</th>
                <th>Table</th>
                <th>Column</th>
                <th>Referenced Table</th>
                <th>Referenced Column</th>
                <th>Delete Rule</th>
                <th>Update Rule</th>
            </tr>
""")
                for rel in self.schema_data['relationships']:
                    f.write(f"""
            <tr>
                <td>{rel['name']}</td>
                <td>{rel['table_schema']}.{rel['table']}</td>
                <td>{rel['column']}</td>
                <td>{rel['referenced_schema']}.{rel['referenced_table']}</td>
                <td>{rel['referenced_column']}</td>
                <td>{rel['delete_rule']}</td>
                <td>{rel['update_rule']}</td>
            </tr>
""")
                f.write("""
        </table>
""")
            else:
                f.write("<p>No relationships found.</p>")
            f.write("</div>")
            
            # Write stored procedures section
            f.write("""
    <div class="section" id="procedures">
        <h2>Stored Procedures</h2>
""")
            if self.schema_data['procedures']:
                for proc in self.schema_data['procedures']:
                    f.write(f"""
        <div class="subsection">
            <h3>{proc['full_name']}</h3>
""")
                    if proc['description']:
                        f.write(f"""
            <p class="description">{proc['description']}</p>
""")
                    if proc['parameters']:
                        f.write("""
            <h4>Parameters</h4>
            <table>
                <tr>
                    <th>Parameter</th>
                    <th>Type</th>
                    <th>Direction</th>
                    <th>Default</th>
                </tr>
""")
                        for param in proc['parameters']:
                            direction = "OUT" if param.get('is_output') else "IN"
                            default = param.get('default_value', '') if param.get('has_default') else ''
                            
                            f.write(f"""
                <tr>
                    <td>{param['name']}</td>
                    <td>{param['type']}</td>
                    <td>{direction}</td>
                    <td>{default}</td>
                </tr>
""")
                        f.write("""
            </table>
""")
                    f.write("""
            <h4>Definition</h4>
            <pre>{}</pre>
        </div>
""".format(proc['definition'] if proc['definition'] else "-- Definition not available"))
            else:
                f.write("<p>No stored procedures found.</p>")
            f.write("</div>")
            
            # Write functions section
            f.write("""
    <div class="section" id="functions">
        <h2>Functions</h2>
""")
            if self.schema_data['functions']:
                for func in self.schema_data['functions']:
                    f.write(f"""
        <div class="subsection">
            <h3>{func['full_name']}</h3>
            <p><strong>Type:</strong> {func['type']}</p>
""")
                    if func['description']:
                        f.write(f"""
            <p class="description">{func['description']}</p>
""")
                    if 'return_type' in func:
                        f.write(f"""
            <p><strong>Returns:</strong> {func['return_type'].get('type', 'Unknown')}</p>
""")
                    if func['parameters']:
                        f.write("""
            <h4>Parameters</h4>
            <table>
                <tr>
                    <th>Parameter</th>
                    <th>Type</th>
                    <th>Default</th>
                </tr>
""")
                        for param in func['parameters']:
                            default = param.get('default_value', '') if param.get('has_default') else ''
                            
                            f.write(f"""
                <tr>
                    <td>{param['name']}</td>
                    <td>{param['type']}</td>
                    <td>{default}</td>
                </tr>
""")
                        f.write("""
            </table>
""")
                    f.write("""
            <h4>Definition</h4>
            <pre>{}</pre>
        </div>
""".format(func['definition'] if func['definition'] else "-- Definition not available"))
            else:
                f.write("<p>No functions found.</p>")
            f.write("</div>")
            
            # Write indexes section
            f.write("""
    <div class="section" id="indexes">
        <h2>Indexes</h2>
""")
            if self.schema_data['indexes']:
                f.write("""
        <table>
            <tr>
                <th>Index</th>
                <th>Table</th>
                <th>Type</th>
                <th>Unique</th>
                <th>Primary Key</th>
                <th>Columns</th>
            </tr>
""")
                for idx in self.schema_data['indexes']:
                    unique = "✓" if idx.get('is_unique') else ""
                    pk = "✓" if idx.get('is_primary_key') else ""
                    
                    f.write(f"""
            <tr>
                <td>{idx['name']}</td>
                <td>{idx['schema']}.{idx['table']}</td>
                <td>{idx['type']}</td>
                <td>{unique}</td>
                <td>{pk}</td>
                <td>{idx['columns']}</td>
            </tr>
""")
                f.write("""
        </table>
""")
            else:
                f.write("<p>No indexes found.</p>")
            f.write("</div>")
            
            # Write HTML footer
            f.write("""
</body>
</html>
""")
        
        print(f"HTML documentation generated: {html_file}")
        return html_file


def main():
    parser = argparse.ArgumentParser(description='Generate database documentation')
    parser.add_argument('--db-type', choices=['mssql', 'db2'], default='mssql', help='Database type')
    parser.add_argument('--server', help='Database server name')
    parser.add_argument('--database', help='Database name')
    parser.add_argument('--port', help='Database port (for DB2)')
    parser.add_argument('--driver', help='ODBC driver name (optional)')
    parser.add_argument('--username', help='Database username')
    parser.add_argument('--password', help='Database password')
    parser.add_argument('--windows-auth', action='store_true', help='Use Windows authentication (MSSQL only)')
    parser.add_argument('--output-format', choices=['markdown', 'excel', 'csv', 'json', 'html', 'all'], 
                        default='all', help='Output format')
    parser.add_argument('--output-dir', default='./db_documentation', help='Output directory')
    parser.add_argument('--erd', action='store_true', help='Generate ERD diagram')
    parser.add_argument('--graphviz-path', help='Path to Graphviz bin directory')
    parser.add_argument('--jdbc-url', help='JDBC URL for DB2 connection (alternative to server/database)')
    
    args = parser.parse_args()
    
    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)
    
def main():
    parser = argparse.ArgumentParser(description='Generate database documentation')
    parser.add_argument('--db-type', choices=['mssql', 'db2'], default='mssql', help='Database type')
    parser.add_argument('--server', help='Database server name')
    parser.add_argument('--database', help='Database name')
    parser.add_argument('--port', help='Database port (for DB2)')
    parser.add_argument('--driver', help='ODBC driver name (optional)')
    parser.add_argument('--username', help='Database username')
    parser.add_argument('--password', help='Database password')
    parser.add_argument('--windows-auth', action='store_true', help='Use Windows authentication (MSSQL only)')
    parser.add_argument('--output-format', choices=['markdown', 'excel', 'csv', 'json', 'html', 'all'],
                        default='all', help='Output format')
    parser.add_argument('--output-dir', default='./db_documentation', help='Output directory')
    parser.add_argument('--erd', action='store_true', help='Generate ERD diagram')
    parser.add_argument('--graphviz-path', help='Path to Graphviz bin directory')
    parser.add_argument('--jdbc-url', help='JDBC URL for DB2 connection (alternative to server/database)')
    parser.add_argument('--schema', help='Schema name for DB2 connection')
    parser.add_argument('--connection-string', help='Full connection string (alternative to individual parameters)')
    
    args = parser.parse_args()
    
    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Build connection string if not provided directly
    connection_string = args.connection_string
    if not connection_string:
        if args.db_type == 'mssql':
            driver = args.driver or 'SQL Server'
            connection_string = f"DRIVER={{{driver}}};"
            
            if args.server:
                connection_string += f"SERVER={args.server};"
            
            if args.database:
                connection_string += f"DATABASE={args.database};"
            
            if args.windows_auth:
                connection_string += "Trusted_Connection=yes;"
            else:
                if args.username:
                    connection_string += f"UID={args.username};"
                if args.password:
                    connection_string += f"PWD={args.password};"
        
        elif args.db_type == 'db2':
            driver = args.driver or 'IBM DB2 ODBC DRIVER'
            connection_string = f"DRIVER={{{driver}}};"
            
            if args.database:
                connection_string += f"DATABASE={args.database};"
            
            if args.server:
                connection_string += f"HOSTNAME={args.server};"
            
            if args.port:
                connection_string += f"PORT={args.port};"
            else:
                connection_string += "PORT=50000;"
            
            connection_string += "PROTOCOL=TCPIP;"
            
            if args.username:
                connection_string += f"UID={args.username};"
            if args.password:
                connection_string += f"PWD={args.password};"
                
            # Add schema to connection string for DB2
            if args.schema:
                connection_string += f"CURRENTSCHEMA={args.schema};"

    try:
        # Create documenter instance
        documenter = DatabaseDocumenter(
            connection_string=connection_string,
            db_type=args.db_type,
            graphviz_path=args.graphviz_path,
            username=args.username,
            password=args.password,
            jdbc_url=args.jdbc_url,
            schema=args.schema
        )
        
        # Extract schema information
        documenter.extract_schema()
        
        # Generate documentation
        if args.output_format == 'all' or args.output_format == 'markdown':
            documenter.generate_markdown(args.output_dir)
        
        if args.output_format == 'all' or args.output_format == 'excel':
            documenter.generate_excel(args.output_dir)
        
        if args.output_format == 'all' or args.output_format == 'csv':
            documenter.generate_csv(args.output_dir)
        
        if args.output_format == 'all' or args.output_format == 'json':
            documenter.generate_json(args.output_dir)
            
        if args.output_format == 'all' or args.output_format == 'html':
            documenter.generate_html(args.output_dir)
        
        # Generate ERD if requested
        if args.erd:
            documenter.generate_erd(args.output_dir)
        
        print(f"Documentation generated successfully in {os.path.abspath(args.output_dir)}")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        traceback.print_exc()
        sys.exit(1)



if __name__ == '__main__':
    main()





