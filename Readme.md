# YouTube-Data-Harvesting-and-Warehousing-using-SQL-MongoDB-and-Streamlit

### Problem Statement:
The objective is to harvest and warehouse data from YouTube channels, including details such as channel information, video statistics, comments, and playlists. Additionally, the goal is to migrate this data into both MongoDB and MySQL databases and provide a Streamlit-based user interface for interacting with the stored data. The project aims to facilitate analysis and querying of YouTube data.

### Technology Stack Used:
1. **Python:**
   - Utilizing Python for scripting and programming tasks.

2. **Libraries and APIs:**
   - `googleapiclient`: Used for interacting with the YouTube Data API to retrieve channel and video information.
   - `pymongo`: Employed for connecting to and interacting with MongoDB.
   - `pymysql`: Utilized for connecting to and interacting with MySQL.
   - `pandas`: Employed for handling and manipulating data in DataFrame structures.
   - `streamlit`: Used to create the web-based user interface for data interaction and visualization.

3. **Databases:**
   - **MongoDB:**
     - A NoSQL database used for storing detailed information about YouTube channels, videos, comments, and playlists.

   - **MySQL:**
     - A relational database used for storing structured data, with tables created for channels, playlists, videos, and comments.

### Approach:
1. **Data Harvesting from YouTube:**
   - Utilized the YouTube Data API to extract information about channels, videos, playlists, and comments.
   - Implemented functions to retrieve channel details, video IDs, video details, comment details, and playlist details.

2. **Data Storage in MongoDB:**
   - Connected to a local MongoDB instance and created a database named "Youtube_Data."
   - Implemented functions to insert channel details, playlist details, video details, and comment details into MongoDB collections.

3. **Data Migration to MySQL:**
   - Established a connection to a local MySQL instance and created a database named "Youtube_pro."
   - Created tables for channels, playlists, videos, and comments in MySQL.
   - Utilized pandas DataFrames to fetch data from MongoDB and inserted it into corresponding MySQL tables.

4. **Streamlit User Interface:**
   - Developed a Streamlit-based user interface for interacting with the harvested and warehoused data.
   - Provided options to collect and store data, migrate data to SQL, and view data from different tables.
   - Implemented SQL queries for analyzing and retrieving specific information from the stored data.

5. **SQL Queries for Analysis:**
   - Implemented various SQL queries to answer specific queries related to channels, videos, and comments.
   - Enabled users to select and execute queries through the Streamlit interface.

### Key Points:
- The project combines the use of NoSQL (MongoDB) and SQL (MySQL) databases for storing different types of YouTube data.
- Streamlit serves as an interactive web-based platform for users to initiate data collection, migration, and analysis.
- The technology stack includes popular Python libraries for data manipulation, database interaction, and web interface development.
- The overall architecture supports both data warehousing and analysis, providing insights into YouTube channel metrics.
