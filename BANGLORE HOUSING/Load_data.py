import os
import pandas as pd
import numpy as np
import mysql.connector

import matplotlib.pyplot as plt

print("Import Dictionary successful")

#===============  update file path   ================

File_path="C:/Users/keert/OneDrive/Desktop/Guvi-project/BANGLORE HOUSING/Luxury_Housing_Bangalore.csv"

df=pd.read_csv(File_path)
#print(df)
#print(df.columns)
#print(df.info())
print('Number of null values',df.isnull().sum())
print('CSV file loaded to dataframe')

#=========Property_ID column is good===========
#=========cleaning  Micro_Market column =============
df['Micro_Market']=df['Micro_Market'].str.title().str.strip()
#print(df['Micro_Market'])
print('title case changed and space removed from Micro_Market column')


# =============No changes needed for Project_Name=========


print('total no of null values in Unit_Size_Sqft column',df['Unit_Size_Sqft'].isnull().sum())
#have 10046 null values
print("Missing values before:", df['Unit_Size_Sqft'].isna().sum())


df['Unit_Size_Sqft']=df['Unit_Size_Sqft'].fillna(df['Unit_Size_Sqft'].mean())
df['Unit_Size_Sqft']=df['Unit_Size_Sqft'].astype(int)

print("Missing values after:", df['Unit_Size_Sqft'].isna().sum())

#==============Ticket_Price_Cr===================
# #Ticket_Price_Cr
#1. Clean up column: remove 'Cr' and '₹', convert to float

df.loc[:,'Ticket_Price_Cr']=(df['Ticket_Price_Cr']
                             .astype(str)
                             .str.replace('Cr','',regex=False)
                             .str.replace('₹', '', regex=False)
                             .str.strip()
                             .astype(float)
                             )
# 2. Convert to float (invalid strings → NaN automatically)
df["Ticket_Price_Cr"] = pd.to_numeric(df["Ticket_Price_Cr"], errors="coerce")

# 3. Replace negative values with NaN
df["Ticket_Price_Cr"] = df["Ticket_Price_Cr"].mask(df["Ticket_Price_Cr"] <= 0, np.nan)

# 4. Fill blanks (NaN) with column mean
mean_value = df["Ticket_Price_Cr"].mean(skipna=True)
df["Ticket_Price_Cr"] = df["Ticket_Price_Cr"].fillna(mean_value)

print('TTL no of null values',df['Ticket_Price_Cr'].isnull().sum())

df['Ticket_Price_Cr'].head(28)

#==========================Price_per_Sqft============================

# 1. Price per sqft (₹ per sqft)
df['Price_per_Sqft'] = (df['Ticket_Price_Cr'] * 1e7) / df['Unit_Size_Sqft']
df['Price_per_Sqft'] =df['Price_per_Sqft'].round(2)

#print(df['Price_per_Sqft'])
#====Transaction_Type====

print(df['Transaction_Type'].value_counts())
#============================
df['NRI_Buyer']=df['NRI_Buyer'].map({'yes':True, 'no':False})
#=======Buyer_Type========

print(df['Buyer_Type'].value_counts())

#======Purchase_Quarter==============

# 1. Convert to datetime (coerce invalid → NaT)
df['Purchase_Quarter'] = pd.to_datetime(df['Purchase_Quarter'], errors='coerce')

# 2. Extract quarter number (1–4), replace NaT with 0
df['Quarter_Number'] = (
    df['Purchase_Quarter']
    .dt.quarter
    .fillna(0)
    .astype(int)
)

#print(df['Quarter_Number'].value_counts())

#===========Connectitivity=============

df['Connectivity_Score']=df['Connectivity_Score'].round(2)
#print(df['Connectivity_Score'])




#=================Amenity_Score=================

df.loc[:, 'Amenity_Score'] = df.groupby('Micro_Market')['Amenity_Score'].transform(lambda x: x.fillna(x.mean()))
df['Amenity_Score']=df['Amenity_Score'].round(2)

print('Total number of null values in Amenity_score column is ', df['Amenity_Score'].isnull().sum())
#print(df['Amenity_Score'])

#==============Buyer_Comments==============

df.loc[:,'Buyer_Comments']=df['Buyer_Comments'].fillna('No Commands')
#print(df['Buyer_Comments'])
print('TTL no of null values', df['Buyer_Comments'].isnull().sum())


#=====================Configuration===================

df.loc[:,'Configuration']=df['Configuration'].astype(str).str.upper()
#print(df['Configuration'])

#================================

#======================= Create Booking_Flag from Transaction_Type=======================
# Let's assume Primary = booked (1), Secondary = not booked (0)
df['Booking_Flag'] = df['Transaction_Type'].str.lower().map({'primary': 1, 'secondary': 0})
#print(df['Booking_Flag'])


#====================================remaing developer_name to Builder ======================

df.rename(columns={"Developer_Name": "Builder"}, inplace=True)
#=====================Duplicates===============

print(df.duplicated().sum())
duplicates = df[df.duplicated()]
#print(duplicates)


df = df.drop_duplicates()
print(df.info())

#print(df)
#===============================================================================================================

# Count frequency of each configuration
counts = df["Configuration"].value_counts()

# --- Donut Chart ---
plt.figure(figsize=(6,6))
wedges, texts, autotexts = plt.pie(
    counts,
    labels=counts.index,
    autopct='%1.1f%%',
    startangle=90,
    wedgeprops=dict(width=0.4)  # <-- creates the "donut hole"
)

# Add title
plt.title("Most In-Demand Housing Configurations")

# Add a white circle in the middle (for a cleaner donut look)
centre_circle = plt.Circle((0,0),0.70,fc='white')
fig = plt.gcf()
fig.gca().add_artist(centre_circle)

#plt.show()

#=====================================================================================================================

print(df.info())
#====================== MY SQL Connection==================


try:

    mydb=mysql.connector.connect(
        host='localhost',
        user='root',
        password='12345',
        database='luxury_housing'
    )
    if mydb.is_connected():
        print ('MYSQl-Python connection successfull')
except mysql.connector.error as err:
    print("error occured",err)

cursor = mydb.cursor()

#====================SQL CODE================
          #USE DB
          #table creation
create_query1= '''CREATE TABLE if not exists Housing_details(Property_ID varchar(255),
                Micro_Market varchar(150),
                Project_Name varchar(150),
                Builder varchar(150),
                Unit_Size_Sqft float,
                Configuration varchar(50),
                Ticket_Price_Cr float,
                Transaction_Type varchar(50),
                Buyer_Type varchar(150),
                Purchase_Quarter date,
                Connectivity_Score float,
                Amenity_Score float,
                Possession_Status varchar(150),
                Sales_Channel varchar(150),
                NRI_Buyer bool,
                Locality_Infra_Score float,
                Avg_Traffic_Time_Min int,
                Buyer_Comments text,
                Price_per_Sqft float,
                Quarter_Number int,
                Booking_Flag int
                                                                      )'''
cursor.execute(create_query1)
mydb.commit()
print("table created successfully")


#values insert

list_of_columns=list(df.columns)
print(list_of_columns)

Insertion_of_Table="""Insert into Housing_details(Property_ID,Micro_Market,Project_Name,Builder,Unit_Size_Sqft,Configuration,Ticket_Price_Cr,Transaction_Type,Buyer_Type,Purchase_Quarter,Connectivity_Score,Amenity_Score,Possession_Status,Sales_Channel,NRI_Buyer,Locality_Infra_Score,Avg_Traffic_Time_Min,Buyer_Comments,Price_per_Sqft,Quarter_Number,Booking_Flag)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
records=df.values.tolist()
try:
   cursor.executemany(Insertion_of_Table,records)
   mydb.commit()
   print("Inserted", cursor.rowcount, "rows successfully.")
except Exception as e:
     print("Errr:", str(e))