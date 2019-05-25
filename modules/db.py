# added a header

import os
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
import datetime

import numpy as np
import pandas as pd
import time

import cfg  # Where GLOBAL_CONSTANTS are defined


def raw2db(source, out_fmt, start, end, mode='a'):
    """Loads source raw data to the specified database format.

    Args:
        mode (str): The export mode.

    Returns:

    Raises:
        AttributeError: The ``Raises`` section is a list of all exceptions
            that are relevant to the interface.
        ValueError: If `param2` is equal to `param1`.

    """

    # Load source
    # TODO(moc): kinds of source
    pass

    # mode:
    if mode == 'a':
        # TODO(moc): Appends data to output file
        pass
    else:
        # TODO(moc): raise some error
        pass

    # TODO(moc): export to DB using specified format
    pass


# %%
def hdf_maxts(hdf_in, key_hdf=None):
    try:
        df_hdf = pd.read_hdf(hdf_in, key=key_hdf, start=-1)
        return df_hdf.reset_index()['Date'].max()
    except (FileNotFoundError, KeyError) as data_not_found:
        return None


# %%
def add_to_hdf(ds_in, hdf_out, key_hdf):
    ts_max = hdf_maxts(hdf_out, key_hdf) or pd.Timestamp.min

    ds_out = ds_in.loc[ds_in.index.get_level_values('Date') > ts_max]
    ds_out.to_hdf(hdf_out, key=key_hdf, format='table', append=True)


# %%
def load_lst_stk():
    """

    Source: http://isin.twse.com.tw/isin/e_C_public.jsp?strMode=2
    Source: http://isin.twse.com.tw/isin/C_public.jsp?strMode=2
    """

    url_in = 'http://isin.twse.com.tw/isin/e_C_public.jsp?strMode=2'

    df00 = pd.read_html(url_in, header=0, encoding='big5')[0]
    cfg.to_stdcol(df00)

    df01 = df00.loc[df00['CFICode'].isin(['ESVUFR'])].copy()
    df01['SId'], df01['SName'] = df01['SId_SName'].str.split(n=1).str


# %%
def craw_stock_day(
        stk_lst=cfg.SET_TSE_PKT,
        date_st='2018-07-01',
        date_end=str(datetime.datetime.now())):
    """Store STOCK_DAY.csv to an HDF file.

    Source: http://www.twse.com.tw/en/page/trading/exchange/STOCK_DAY.html
    """

    for stk_i in iter(stk_lst):
        hdf_out = os.path.join(cfg.PATH_RAW, f'tw_{stk_i}.h5')
        key_hdf = f'stock_day'
        dt_end = parse(date_end)

        # Determine the first date to use in the while loop
        ts_hdfmax = hdf_maxts(hdf_out, key_hdf)

        if ts_hdfmax is not None:
            dt_loop = ts_hdfmax.to_pydatetime() + relativedelta(day=1)
        else:
            dt_loop = parse(date_st) + relativedelta(day=1)

        while dt_loop <= dt_end:
            csv_in = ('http://www.twse.com.tw/en/exchangeReport/STOCK_DAY?'
                      + f'response=csv&date={dt_loop:%Y%m%d}&stockNo={stk_i}')
            try:
                df00 = pd.read_csv(csv_in, engine='python',
                                   header=1, thousands=',')
            except ValueError:
                print('Loading failed:', csv_in)
            else:
                print('Loading', csv_in)

                # Standardize column names
                cfg.to_stdcol(df00)

                # Assign SId
                df01 = df00.assign(SId=stk_i)

                # Assign Date
                df01['Date'] = pd.to_datetime(df01['Date'], errors='coerce')

                # Set Date and SId to index
                df01 = df01.set_index(['Date', 'SId'])

                # Remove redundant columns and rows
                df02 = df01.dropna(1, 'all').dropna(thresh=2)

                # Convert all columns to float
                df02 = df02.apply(pd.to_numeric, errors='coerce')

                # Store final dataframe to HDF
                add_to_hdf(df02, hdf_out, key_hdf)
            finally:
                # Go to next loop
                dt_loop = dt_loop + relativedelta(months=+1)
                time.sleep(2.7)


# %%
def craw_FMTQIK(
        date_st='2018-07-01',
        date_end=str(datetime.datetime.now())):
    """Store FMTQIK.csv to an HDF file.

    Source: http://www.twse.com.tw/en/page/trading/exchange/FMTQIK.html
    Source: http://www.twse.com.tw/zh/page/trading/exchange/FMTQIK.html
    """

    stk_i = 'tse'
    hdf_out = os.path.join(cfg.PATH_RAW, f'tw_{stk_i}.h5')
    key_hdf = f'FMTQIK'
    dt_end = parse(date_end)

    # Determine the first date to use in the while loop
    ts_hdfmax = hdf_maxts(hdf_out, key_hdf)

    if ts_hdfmax is not None:
        dt_loop = ts_hdfmax.to_pydatetime() + relativedelta(day=1)
    else:
        dt_loop = parse(date_st) + relativedelta(day=1)

    while dt_loop <= dt_end:
        # Assign csv file to read
        csv_in = ('http://www.twse.com.tw/en/exchangeReport/FMTQIK?'
                  + f'response=csv&date={dt_loop:%Y%m%d}')
        try:
            df00 = pd.read_csv(csv_in, engine='python',
                               header=1, thousands=',')
        except ValueError:
            print('Loading failed:', csv_in)
        else:
            print('Loading', csv_in)

            # Standardize column names
            cfg.to_stdcol(df00)

            # Assign Date
            df01 = df00.assign(SId=stk_i)

            # Assign SId
            df01['Date'] = pd.to_datetime(df01['Date'], errors='coerce')

            # Set Date and SId to index
            df01 = df01.set_index(['Date', 'SId'])

            # Remove redundant columns and rows
            df02 = df01.dropna(1, 'all').dropna(thresh=2)

            # Convert all columns to float
            df02 = df02.apply(pd.to_numeric, errors='coerce')

            # Store resulting dataframe to HDF
            add_to_hdf(df02, hdf_out, key_hdf)
        finally:
            dt_loop = dt_loop + relativedelta(months=+1)
            time.sleep(2.7)


# %%
def craw_TWTASU(
        date_st='2018-07-01',
        date_end=str(datetime.datetime.now())):
    """Store TWTASU.csv to an HDF file.

    Source: http://www.twse.com.tw/en/page/trading/exchange/TWTASU.html
    Source: http://www.twse.com.tw/zh/page/trading/exchange/TWTASU.html
    """

    hdf_out = os.path.join(cfg.PATH_RAW, f'twtasu.h5')
    key_hdf = f'TWTASU'
    dt_end = parse(date_end)

    # Determine the first date to use in the while loop
    ts_hdfmax = hdf_maxts(hdf_out, key_hdf)

    if ts_hdfmax is not None:
        dt_loop = ts_hdfmax.to_pydatetime() + relativedelta(days=+1)
    else:
        dt_loop = parse(date_st)

    while dt_loop <= dt_end:
        # Read in the csv file
        csv_in = ('http://www.twse.com.tw/exchangeReport/TWTASU?'
                  + f'response=csv&lang=en&date={dt_loop:%Y%m%d}')
        try:
            df00 = pd.read_csv(csv_in, engine='python',
                               header=2, thousands=',')
        except ValueError:
            print('Loading failed:', csv_in)
        else:
            print('Loading', csv_in)

            # Standardize column names
            cfg.to_stdcol(df00)

            # Assign Date
            df01 = df00.assign(Date=pd.to_datetime(dt_loop))

            # Assign SId
            df01['SId'] = df01['SId'].str.replace(r'[^\w\s]', '')

            # Set Date and SId to index
            df01 = df01.set_index(['Date', 'SId'])

            # Remove redundant columns and rows
            df02 = df01.dropna(1, 'all').dropna(thresh=2)

            # Convert all columns to float
            df02 = df02.apply(pd.to_numeric, errors='coerce')

            # Store resulting dataframe to HDF
            add_to_hdf(df02, hdf_out, key_hdf)
        finally:
            dt_loop = dt_loop + relativedelta(days=+1)
            time.sleep(2.7)


"""
pd.read_hdf(r'C:\Users\chris\OneDrive\Programs\Python\pydata\tw\tw_2330.h5', start = -100)



dt = datetime.datetime.now()
dt.year
dt.month


id_list = ['2303', '2330', '1234', '3006', '2412']  # Inout the stock IDs
now = datetime.datetime.now()
year_list = range(2007, now.year + 1)  # Since 2007 to this year
month_list = range(1, 13)  # 12 months


for stk_i in id_list:
    for year in year_list:
        for month in month_list:
            if (dt.year == year and month > dt.month): break  # Break loop while month over current month

            sid = str(stk_i)
            yy = str(year)
            mm = month
            directory = 'D:/stock' + '/' + sid + '/' + yy + '/'  # Setting directory
            filename = str(yy) + str("%02d"%mm) + '.csv'  # Setting file name
            smt = get_webmsg(year, month, stk_i)  # Put the data into smt
            makedirs(year, month, stk_i)  # Create directory function
            write_csv(stk_i, directory, filename, smt)  # Write files into CSV
            time.sleep(1)





# ###http://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date=20170605&stockNo=2330

# Standard web crawing process
def get_webmsg(year, month, stk_i):
    date = str(year) + "{0:0=2d}".format(month) + '01'  ## format is yyyymmdd
    sid = str(stk_i)
    url_twse = 'http://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date=' + date + '&stockNo=' + sid
    res = requests.post(url_twse,)
    soup = bs(res.text, 'html.parser')
    smt = json.loads(soup.text)  # Convert data into json
    return smt


def write_csv(stk_i, directory, filename, smt):
    writefile = directory + filename  # Set output file name
    outputFile = open(writefile, 'w', newline = '')
    outputWriter = csv.writer(outputFile)
    head = ''.join(smt['title'].split())
    a = [head, ""]
    outputWriter.writerow(a)
    outputWriter.writerow(smt['fields'])
    for data in (smt['data']):
        outputWriter.writerow(data)

    outputFile.close()


# Create a directory in the current one doesn't exist
def makedirs(year, month, stk_i):
    sid = str(stk_i)
    yy = str(year)
    mm = str(month)
    directory = 'D:/stock' + '/' + sid + '/' + yy
    if not os.path.isdir(directory):
        os.makedirs (directory)  # os.makedirs able to create multi folders
"""