import pandas as pd

nzta_data = 'https://nztaopendata.blob.core.windows.net/motorvehicleregister/Fleet-data-all-vehicle-years.zip'
zipfilename = 'nzta_data.zip'
datdir = 'nzta_data'
datfile = 'Fleet-31Jan2023.csv'
atypical_values = ['TRAILER', 'CARAVAN', 'FACTORY BUILT', 'HOMEBUILT', 'BRIFORD']
first_year = 1950
last_year = 2022

    
 

def do_main():
    df = pd.read_csv(nzta_data, compression='zip') ##read streaming data from URL
    print('raw data row count:' + str(df.shape[0]))
    df_typical = df.loc[~df['MAKE'].isin(atypical_values)] # identify the data excluding the non-typical makes
    df_makes = df_typical.groupby(['MAKE'])['MAKE'].count() #get that list of makes with occurrence counts
    print('make count:' + str(df_makes.shape[0]))
    df_topmakes = df_makes.nlargest(20)  #we only want the 20 most common though
     
    df_years = pd.Series(data=range(first_year,last_year+1), index=range(first_year,last_year+1), name='FIRST_NZ_REGISTRATION_YEAR').to_frame() #generate approved year range
    df_year_makes = df_years.merge(df_topmakes.index.to_frame(), how = "cross") #produce the cartesian of years and makes
    

    df_data = df_typical[df_typical['MAKE'].isin(df_topmakes.index)] #get the source rows with only the most common makes
    print('filtered data row count:' + str(df_data.shape[0]))
    print('filtered data column count:' + str(df_data.shape[1]))
    df_agg = df_data.groupby(['MAKE','FIRST_NZ_REGISTRATION_YEAR'], as_index=False)['VIN11'].nunique(dropna=True) #give me the count of unique VINs, dropping blanks
    print('aggregated data row count:' + str(df_agg.shape[0]))  #counts before infilling with cartesian
    #print('aggregated data column count:' + str(df_agg.shape[1]))
    #print(df_agg)

    
    df_output = df_year_makes.merge(df_agg, how='left', left_on= ['MAKE','FIRST_NZ_REGISTRATION_YEAR'], right_on = ['MAKE','FIRST_NZ_REGISTRATION_YEAR']).fillna(0) #join counts against cartesian to produce filler rows
    df_output.rename(columns={'VIN11' : 'NUMBER_OF_REGISTRATIONS'}, inplace=True) #rename the "VIN11" column to match output specification
    print('fully populated data row count:' + str(df_output.shape[0]))

    df_output.to_csv('output.csv',index=False) #drop the data out to CSV
    




if __name__ == "__main__":
    do_main()