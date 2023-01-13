import psycopg2
from datetime import date as Date
import pandas as pd


def get_date(date1,date2):
    d1 =  date1.split("/")
    d2 =  date2.split("/")

    d1 = [int(i) for i in d1][::-1]
    d2 = [int(i) for i in d2][::-1]

    d1 = Date(*d1)
    d2 = Date(*d2)

    return d1,d2

def get_io_difference(date1,date2,cursor):
    """
    Parameters:
    date1 (str): "day/month/year"
    date2 (str): "day/month/year"
    cursor: psycopg2 cursor

    Parameters example: 
    get_io_difference("2/1/2023","5/1/2023",cur)
  
    Returns:
    pandas.dataframe

    """

    date1,date2 = get_date(date1,date2) 

    cur.execute("""SELECT date,OI,strike,instrument_type from test_assignment where date = %s """,[date1])
    date1_data = cur.fetchall()
    cur.execute("""SELECT date,OI,strike,instrument_type from test_assignment where date = %s """,[date2])
    date2_data = cur.fetchall()


    column_names = ["date","IO","strike","instrument_type"]
    df_date1 = pd.DataFrame(date1_data,columns=column_names)
    df_date2 = pd.DataFrame(date2_data,columns=column_names)

    df_date1_ce = df_date1[df_date1["instrument_type"] == "CE"]
    df_date1_pe = df_date1[df_date1["instrument_type"] == "PE"]
    df_date2_ce = df_date2[df_date2["instrument_type"] == "CE"]
    df_date2_pe = df_date2[df_date2["instrument_type"] == "PE"]
    df = pd.DataFrame(index=df_date2_pe["strike"])

    df_date1_ce.set_index("strike",inplace=True)
    df_date1_pe.set_index("strike",inplace=True)
    df_date2_ce.set_index("strike",inplace=True)
    df_date2_pe.set_index("strike",inplace=True)


    df["IO difference CE"] = df_date2_ce["IO"] - df_date1_ce["IO"] 
    df["IO difference PE"] = df_date2_pe["IO"] - df_date1_pe["IO"] 

    return df


if __name__ == '__main__':

    conn = psycopg2.connect(
        host="devtradingsagedb-do-user-12481132-0.b.db.ondigitalocean.com",
        database="defaultdb",
        user="doadmin",
        password="AVNS_AZ-3Q1oUpp9WnsReBBX",
        port="25060"
    )

    cur = conn.cursor()

    date1 = "2/1/2023"
    date2 = "5/1/2023"

    df = get_io_difference(date1,date2,cur)
    print(df.head())





