#First we import the argparse in order to give all our arguments to our function
import argparse
import pandas as pd
import os

#Functions
    #Extraction: Read the files function

def data_extraction(directory):

    # We create a dictionary that will contain our tables in order to make our job easier later.
    tables={} 
    
    #We make a list of the expected files
    expected_files = ["demographics", "epidemiology", "health","hospitalizations","index","vaccinations"]
    #We extract only the list of FILES, without folders.
    files=[file for file in os.listdir(directory) if (os.path.isfile(os.path.join(directory,file)) and file in expected_files)]
    
    # We need to create a for loop to go through the files in our path and save them as a data frame in our directory
    for file in files:
        #We store the DataFrames in our table.
        tables[file]=pd.read_csv(os.path.join(directory,file))
    
    return tables


    #Transforming

        #We create the column dictionary with the list of columns we will keep
column_dict={
    'demographics': ['location_key','population','population_male','population_female','population_age_00_09','population_age_10_19','population_age_20_29','population_age_30_39','population_age_40_49','population_age_50_59','population_age_60_69','population_age_70_79','population_age_80_and_older'],
    'epidemiology':['date','location_key','new_confirmed','new_deceased'],
    'health':['location_key','life_expectancy'],
    'hospitalizations':['date','location_key','new_hospitalized_patients'],
    'index':['location_key','country_name'],
    'vaccinations':['date','location_key','new_persons_fully_vaccinated']
}

        # Function to extract our columns:
def assign_columns(tables:dict, column_dict:dict):

    # We go through each table in our dictionaries and reassign the columns we want for that table.
    for key, value in tables.items():
        if key in column_dict:
            tables[key]=value[column_dict[key]]

    return tables

        # Function that will transform our location_key to contain only the country code (First 2 letters)
def loc_key_transformation (tables:dict):
    location='location_key'
    for key,value in tables.items():
        if location in value.columns:
            value[location]=value[location].str[:2]
    tables[key]=value
    return tables

        # Function that will drop duplicates and empty rows
def dropping_dup_empty (tables: dict):
    for key,value in tables.items():
        value=value.drop_duplicates()
        value=value.dropna(how='all')
        tables[key]=value
    return tables

        # Function that will eliminate rows that do not have a date or a location_key
def drop_empty(tables:dict):

    for key,value in tables.items():
        #Check if the table has the column location key before applying the drop function to avoid errors.
        if 'location_key' in value.columns: 
            value=value.dropna(subset=['location_key'])
        #Check if the table has the column date before applying the drop function to avoid errors.
        if 'date' in value.columns:
            value=value.dropna(subset=['date'])
        #Reassigning the corrected table in our tables dictionary
        tables[key]=value    
        
    return tables

        #Function that fill will missing values with the median.
def fill_median(tables:dict):
    #List of columns we will be changing
    change_column=['population','population_male','population_female','population_age_00_09',
                 'population_age_10_19','population_age_20_29','population_age_30_39','population_age_40_49',
                 'population_age_50_59','population_age_60_69','population_age_70_79','population_age_80_and_older',
                 'life_expectancy']
    
    #We will do a for loop to go through each column inside our table and apply the changes were there need to be
    
    for key,value in tables.items(): #Accesing the tables in our dictionary
        for col in change_column: #Accesing the list of columns
            if col in value.columns: #Checking if the column is in our table
                med=value.groupby('location_key')[col].median() #We create a table with the values grouped with their median by location
                value[col]=value[col].fillna(value['location_key'].map(med)) #Filling with the median of our column with the corresponding value.
                #We use the map function which will locate the location key in our table and match it in our med table (with the grouped values),
                #then it will return the median value
        
        tables[key]=value
        
    return tables


       # Function to fill values with 0, assuming that NaN's are due to no cases being reported
def fill_zero(tables:dict):
    #List of columns we will be changing
    change_column=['new_confirmed','new_deceased','new_hospitalized_patients','new_persons_fully_vaccinated']
    
    #We will do a for loop to go through each column inside our table and apply the changes were there need to be
    
    for key,value in tables.items(): #Accesing the tables in our dictionary
        for col in change_column: #Accesing the list of columns
            if col in value.columns: #Checking if the column is in our table
                value[col]=value[col].fillna(0) #Filling with 0.
        
        tables[key]=value
        
    return tables

        #Functions to create our week column
            #Function to transform date values into date format
def date_transformation(tables:dict):
    dt='date' #Assign name to a variable for simplicity
    for key,value in tables.items(): #For loop to go through our tables
        if dt in value.columns: #Checking tables that have a column 'date'
            value[dt]=pd.to_datetime(value[dt]) #Transforming the column into datetime
    tables[key]=value #Assigning the new table values to their corresponding key
    return tables

            #Function to create the week string according to the date
def week_dates(dt):
    st_date=(dt-pd.Timedelta(days=dt.weekday())).date() #First, we calculate the starting date of the week to which this date belongs to.
    end_date=(dt+pd.Timedelta(days=6-dt.weekday())).date() #Then, we calculate the ending date of the week to which this date belongs to.
    return f"{st_date}/{end_date}" #Format result

            #Function to add the new column
def week_column(tables:dict):
    for key,value in tables.items(): #For loop to go through our tables

        if 'date' in value.columns: #Checking tables that have a column 'date'
            
            value["week"] = value['date'].apply(week_dates) #Appy previous function to add column week with values

            value = value.drop(columns=['date']) #Drop date column
        tables[key]=value  #Assign new table to the key
    return tables

        #Function to filter dates
def filter_date_country(tables:dict,start:str, end:str, countries:list):
#We filter countries
    if 'index' in tables and countries:
        countries = [country.lower() for country in countries]
        tables['index']=tables['index'][tables['index'].country_name.str.lower().isin(countries)]
    for key,value in tables.items():
        if 'date' in value.columns: #Check if there is a date column in our table
            value=value[(value['date']>=start)&(value['date']<=end)]
        tables[key]=value
    return tables

    #Data Cleaning summary function
def data_cleaning(tables:dict, col_dict:dict,start:str,end:str,countries:list):
    tables=assign_columns(tables, col_dict) #Extracting relevant columns
    tables=loc_key_transformation(tables) #Transforming the location key to keep the country code (first 2 letters)
    tables=dropping_dup_empty(tables) #dropping duplicates and completely empty rows
    tables=drop_empty(tables) #dropping rows that have and empty location_key or an empty date
    tables=date_transformation(tables) #Transforming dates into date format
    tables=filter_date_country(tables,start,end,countries) #Filtering dates and countries with the input arguments
    tables=week_column(tables) #We add our date column
    tables=fill_median(tables)#We fill our missing values with the median (population)
    tables=fill_zero(tables) #We fill missing values with 0 (new cases)
    return tables

        #Function to make the aggregations
def aggregations (tables:dict):

    for key,value in tables.items():
        if 'week' in value.columns and 'location_key' in value.columns:
            value=value.groupby(by=['week','location_key'], as_index=False).sum()
        elif 'location_key' in value.columns and 'week' not in value.columns:
            if key=='health':
                value=value.groupby(by='location_key', as_index=False).mean()
            else:
                value=value.groupby(by='location_key', as_index=False).sum()
        tables[key]=value
    return tables

        #Function to make the joins
def joins(tables:dict):
    #First we join epidemiology table with hospitalization table with a left outer join to not lose values
    t1=pd.merge(tables['epidemiology'],tables['hospitalizations'],how="left", on=['week','location_key'])
    #Then we join the resulting table with vaccinations table with a left outer join to not lose values
    t2=pd.merge(t1,tables['vaccinations'],how="left",on=['week','location_key'])
    #Then we join the resulting table with health table with a left outer join to not lose values
    t3=pd.merge(t2,tables['health'],how="left")
    #Then we join our info tables demographics and index with an inner join
    t4=pd.merge(tables['demographics'],tables['index'],how="inner")
    #Then we get our final table with an inner join of t3 and t4
    final_table=pd.merge(t3,t4,how="inner")
    return final_table


        #Function to export the table
def export(tables:pd.DataFrame,directory:str):
    current=os.getcwd()
    if os.path.exists(directory):
        path1=os.path.join(directory,"macrotable.csv")
        tables.to_csv(path1,index=False)
    else:
        path2=os.path.join(current,"macrotable.csv")
        tables.to_csv(path2, index=False)


#We create the main function to execute all of our functions
def main(path:str, o:str, start:str, end:str, countries:list):
    #First, we extract the data.
    data=data_extraction(path)

    #Then, we will clean the data
    data=data_cleaning(data,column_dict,start,end,countries)

    #Then, we do the aggregation operations
    data=aggregations(data)

    #Then, we do the joins
    table=joins(data)

    #Finally, we export our table
    export(table,o)

#Only execute main() if it's being executed from the command line
if __name__=="__main__":

    parser = argparse.ArgumentParser()

    #We declare the arguments me want to catch
    parser.add_argument('path',type=str, help="This is the location of the folder containing the input files")
    parser.add_argument('-s','--start',type=str, default='2020-01-02',help="This is the starting date")
    parser.add_argument('-e','--end',type=str,default='2022-08-22', help="This is the ending date")
    parser.add_argument('-o','--output',type=str,default=os.getcwd(), help="This is output path. If not set, macrotable is stored in the current directory by default")
    parser.add_argument('-c','--countries', type=str, nargs='*',default=[],help="This is the countries that you want to show up")

    args = parser.parse_args()

    main(args.path, args.output, args.start, args.end, args.countries)

    


    