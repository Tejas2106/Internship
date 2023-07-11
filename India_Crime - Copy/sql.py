import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect('crime_database.db')  # Replace with your desired database name

# Create a cursor object
cursor = conn.cursor()

# Task 3.1: Insert records from "42_District_wise_crimes_committed_against_women_2001_2012.csv" into a table
create_table_query = '''
CREATE TABLE crimes_against_women (
    State TEXT,
    District TEXT,
    Year INTEGER,
    Rapes INTEGER,
    Kidnappings INTEGER
);
'''
cursor.execute(create_table_query)

df_women_crimes = pd.read_csv('42_District_wise_crimes_committed_against_women_2001_2012.csv')
df_women_crimes.to_sql('crimes_against_women', conn, if_exists='append', index=False)

# Task 3.2: Write SQL query to find the highest number of rapes & kidnappings that happened in which state, District, and year
query_highest_crimes = '''
SELECT State, District, Year, MAX(Rapes) AS MaxRapes, MAX(Kidnappings) AS MaxKidnappings
FROM crimes_against_women
GROUP BY State, District, Year
ORDER BY MaxRapes DESC, MaxKidnappings DESC
LIMIT 1;
'''
cursor.execute(query_highest_crimes)
highest_crimes_result = cursor.fetchall()
print("Highest Crimes:")
print(highest_crimes_result)

# Task 3.3: Write SQL query to find all the lowest number of rapes & kidnappings that happened in which state, District, and year
query_lowest_crimes = '''
SELECT State, District, Year, MIN(Rapes) AS MinRapes, MIN(Kidnappings) AS MinKidnappings
FROM crimes_against_women
GROUP BY State, District, Year
ORDER BY MinRapes ASC, MinKidnappings ASC;
'''
cursor.execute(query_lowest_crimes)
lowest_crimes_result = cursor.fetchall()
print("Lowest Crimes:")
print(lowest_crimes_result)

# Task 3.4: Insert records from "02_District_wise_crimes_committed_against_ST_2001_2012.csv" into a new table
create_table_query_st = '''
CREATE TABLE crimes_against_ST (
    State_UT TEXT,
    District TEXT,
    Year INTEGER,
    Dacoity_Robbery INTEGER
);
'''
cursor.execute(create_table_query_st)

df_st_crimes = pd.read_csv('02_District_wise_crimes_committed_against_ST_2001_2012.csv')
df_st_crimes.to_sql('crimes_against_ST', conn, if_exists='append', index=False)

# Task 3.5: Write SQL query to find the highest number of dacoity/robbery in which district
query_highest_dacoity_robbery = '''
SELECT District, MAX(Dacoity_Robbery) AS MaxDacoityRobbery
FROM crimes_against_ST
GROUP BY District
ORDER BY MaxDacoityRobbery DESC
LIMIT 1;
'''
cursor.execute(query_highest_dacoity_robbery)
highest_dacoity_robbery_result = cursor.fetchall()
print("Highest Dacoity/Robery:")
print(highest_dacoity_robbery_result)

# Task 3.6: Write SQL query to find in which districts (All) the lowest number of murders happened
query_lowest_murders = '''
SELECT District, MIN(Rapes) AS MinRapes
FROM crimes_against_women
GROUP BY District
HAVING COUNT(*) = (SELECT COUNT(DISTINCT District) FROM crimes_against_women)
ORDER BY MinRapes ASC;
'''
cursor.execute(query_lowest_murders)
lowest_murders_result = cursor.fetchall()
print("Districts with Lowest Murders:")
print(lowest_murders_result)

# Task 3.7: Write SQL query to find the number of murders in ascending order in district and yearwise
query_murders_asc = '''
SELECT District, Year, Rapes AS Murders
FROM crimes_against_women
ORDER BY Murders ASC, District ASC, Year ASC;
'''
cursor.execute(query_murders_asc)
murders_asc_result = cursor.fetchall()
print("Murders in Ascending Order:")
print(murders_asc_result)

# Close the cursor and connection
cursor.close()
conn.close()
