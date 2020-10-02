import requests
from datetime import date, timedelta, datetime
import zipfile
import pandas as pd
import os

csv_path = "./csvs"
zip_path = "./zip"


def file_cleanup():
    try:
        filename = os.listdir()
        for i in filename:
            if i.endswith((".zip",".csv")):
                os.remove((i))
    except Exception as e:
        print(e)


def form_file_name(delta):
    #curr_day = str(date.today().day) if len(str(date.today().day)) == 2 else "0"+str(date.today().day)
    #curr_month = str(date.today().month).upper()
    #curr_year = str(date.today().year)
    test_date = date.today() - timedelta(delta)
    #sd = date.today().strftime("%Y-%b-%d").split("-")
    sd = test_date.strftime("%Y-%b-%d").split("-")
    url = f"https://archives.nseindia.com/content/historical/EQUITIES/{sd[0]}/{sd[1].upper()}/cm{sd[2]}{sd[1].upper()}{sd[0]}bhav.csv.zip"
    file_name =  sd[1].upper()+"_"+sd[2]+"_"+sd[0]
    csv_name =  f"cm{sd[2]}{sd[1].upper()}{sd[0]}bhav.csv"
    return url, file_name, csv_name


def download_and_unzip(url,save_path_name,chunk_size=128):
    #url = "https://archives.nseindia.com/content/historical/EQUITIES/2020/SEP/cm25SEP2020bhav.csv.zip"
    r = requests.get(url, timeout= 10, stream=True)
    try:
        with open("zips/"+save_path_name+".zip","wb") as fd:
            for chunk in r.iter_content(chunk_size=chunk_size):
                fd.write(chunk)
    except Exception  as e:
        print(e)
    with zipfile.ZipFile("zips/"+save_path_name+".zip", "r") as zip_ref:
        zip_ref.extractall("./csvs/")


def pandas_ops(sorted_dates):
    for i in sorted_dates:
        csv_d = i.strftime("%Y-%b-%d").split("-")
        csv_name = f"cm{csv_d[2]}{csv_d[1].upper()}{csv_d[0]}bhav.csv"
        csv_path = os.path.join(os.path.dirname(__file__),'csvs',csv_name)
        df = pd.read_csv(csv_path, index_col="SYMBOL")
        #print(df.head())
        main_df = df[(df.SERIES == "EQ" )][["HIGH","LOW"]]
        #print(main_df.columns)
        yield main_df


def get_big_df(df_generator):
    big_df = pd.concat(df_generator, axis=1)
    return big_df
    #big_df = big_df.T
    #big_df.to_csv("test4.csv")
    #print(big_df.loc["TCS"]["HIGH"].max())
    #rint(big_df.loc["TCS"]["HIGH"].argmax())

def get_low_high_values(big_df, symbol):
    high = big_df.loc[symbol]["HIGH"].max()
    low = big_df.loc[symbol]["LOW"].min()
    hi = big_df.loc[symbol]["HIGH"].argmax()
    li = big_df.loc[symbol]["LOW"].argmin()
    return high, low, hi, li




def get_file_count(path=csv_path):
    return len(os.listdir(path))


def last_90_day_data():
    for i in range(150):
        if get_file_count(csv_path) >= 90:
            break
        else:
            try:
                url, path, csv_name = form_file_name(delta=i)        #print(url)
                download_and_unzip(url, path)
                get_file_count(csv_path)
            except Exception as e:
                print(e)

def get_latest_beta():
    full_path_l = [os.path.join(os.path.dirname(__file__),'csvs',i) for i in os.listdir(csv_path)]
    latest = max(full_path_l, key=os.path.getctime)
    print(latest)


def get_last_csv():
    csv_dates = []
    for i in os.listdir(csv_path):
        i = i[2:-8]
        #print(i)
        i_date = datetime.strptime(i,"%d%b%Y")
        csv_dates.append(i_date)
        #print(i_date, type(i_date))
    last_date = min(csv_dates)
    ld = last_date.strftime("%Y-%b-%d").split("-")
    last_csv = f"cm{ld[2]}{ld[1].upper()}{ld[0]}bhav.csv"
    print("last date is ", last_date, last_csv)
    return last_csv

def remove_last_csv(last_csv):
    csv_path = os.path.join(os.path.dirname(__file__),'csvs',last_csv)
    print(os.remove(csv_path))
    print(get_file_count())


def sort_dates():
    csv_dates = []
    for i in os.listdir(csv_path):
        i = i[2:-8]
        #print(i)
        i_date = datetime.strptime(i,"%d%b%Y")
        csv_dates.append(i_date)
        #print(i_date, type(i_date))
    date_list = sorted(csv_dates , reverse=True)
    return date_list





if __name__=="__main__":
    #last_csv = get_last_csv()
    #remove_last_csv(last_csv)
    sorted_dates = sort_dates()
    df_gen = pandas_ops(sorted_dates)
    big_df = get_big_df(df_gen)
    for i in big_df.index.values:
        high, low, hi, li = get_low_high_values(big_df, i)
        print(i, high, low, hi , li)





