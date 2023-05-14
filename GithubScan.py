import streamlit as st
import plotly.express as px
import pandas as pd
import numpy
import datetime
import os 
import time
import pickle
import yfinance as yf
import requests

st.set_page_config(page_title='Stock scan', page_icon=':bar_chart:',layout="wide")

# CSS to inject contained in a string
hide_dataframe_row_index = """
            <style>
            .row_heading.level0 {display:none}
            .blank {display:none}
            </style>
            """
# Inject CSS with Markdown
st.markdown(hide_dataframe_row_index, unsafe_allow_html=True)

today=datetime.datetime.now()

#################################
######## Functions uncached #######
#################################
def getabove50sma(stock):
    if stock in dic_scaned.keys():
        if dic_scaned[stock]['Close'].iloc[-1]>dic_scaned[stock]['SMA_50'].iloc[-1]: return '✔'
        else: return '❌'
    else: return ''

def getabove34ema(stock):
    if stock in dic_scaned.keys():
        if dic_scaned[stock]['Close'].iloc[-1]>dic_scaned[stock]['EMA_34'].iloc[-1]: return '✔'
        else: return '❌'
    else: return ''
    
def getRainbowlogic(stock):
    if stock in dic_scaned.keys():
        df = dic_scaned[stock]
        condition_1 = df['EMA_8'].iloc[-1] > df['EMA_21'].iloc[-1] > df['EMA_34'].iloc[-1] > df['EMA_55'].iloc[-1] > df['EMA_89'].iloc[-1]
        if condition_1: return '✔'
        else: return '❌'
    else: return ''

def getOverextended(stock):
    if stock in dic_scaned.keys():
        df = dic_scaned[stock]
        conditionOE = df['Close'].iloc[-1] >= df['UpperBand2'].iloc[-1] 
        if conditionOE: return '✔'
        else: return ' '
    else: return ''

def getActionzone(stock):
    if stock in dic_scaned.keys():
        df = dic_scaned[stock]
        conditionAZ = (df['Close'].iloc[-1] <= df['UpperBand1'].iloc[-1]) 
        if conditionAZ: return '✔'
        else: return ' '
    else: return ''

def STPhaseIndices(SeriesPrice, Series200SMA, Series8Ema, Series21Ema):
    phase, focus = 'Indeterminate', 'Indeterminate'
    
    if (SeriesPrice.iloc[-1:] > Series200SMA.iloc[-1:])[0]: LTtrend='UPTREND'
    else: LTtrend='DOWNTREND'
    
    
    if Series8Ema.iloc[-1]>Series21Ema.iloc[-1]:
        if (SeriesPrice.iloc[-1]>Series21Ema.iloc[-1]): 
            if (SeriesPrice.iloc[-2]>Series21Ema.iloc[-2]): phase = 'BULL phase - Strong Uptrend'
            else: phase = 'BULL phase - Uptrend'
    else:
        if (SeriesPrice.iloc[-1]<Series21Ema.iloc[-1]):
            if (SeriesPrice.iloc[-2]<Series21Ema.iloc[-1]): phase = 'BEAR phase - Strong Downtrend'
            else: phase = 'BEAR phase - Downtrend'
            
    if LTtrend=='Uptrend':
        if phase in ['BULL phase - Strong Uptrend','BULL phase - Uptrend']: focus = 'Almost exclusively on long trades (buy the dip).'
        elif phase in ['BEAR phase - Strong Downtrend', 'BEAR phase - Downtrend']: focus = 'Get some hedges. Be on the lookout for 1 or 2 bear trades. Wait for deeper pullbacks on bull setups (eg. 50SMA). Avoid getting too bearish'    
        else: focus = 'Avoid getting too bearish'
    else:
        if phase in ['BULL phase - Strong Uptrend','BULL phase - Uptrend']: 
            focus = 'We can expect a countertrend rally, could last days/weeks. Be cautious about being completely bearish in portfolio. Be sure to take some bullish trades. The strongest rallies occur in bear markets (short-covering).'
        elif phase in ['BEAR phase - Strong Downtrend', 'BEAR phase - Downtrend']: 
            focus = 'Want to be focused on bearish trades. Buy puts on rallies. Bullish trades are less likely to work well. '
    
    return LTtrend, phase, focus  

def LTemoji(index):
    val = sectorsTrends[index][0]
    if val == 'UPTREND': emoj = '🟩'
    elif val == 'DOWNTREND': emoj = '🟥'
    else: emoj = '⬜'
    return emoj

def STemoji(index):
    phase = sectorsTrends[index][1]
    if phase in ['BULL phase - Strong Uptrend','BULL phase - Uptrend']: emoj = '🟩'
    elif phase in ['BEAR phase - Strong Downtrend', 'BEAR phase - Downtrend']: emoj = '🟥'
    else: emoj = '⬜'
    return emoj

#################################
######## Functions cached #######
#################################

@st.cache_data
def getEcCalendar(today):
    path = './cache_csv/'+'EconCalendar'+today.strftime("%m-%d-%Y")+'.pickle'
    check_file = os.path.isfile(path)
    if check_file:
        with open(path, 'rb') as handle:
            data = pickle.load(handle)
    else:
        url = 'https://economic-calendar.tradingview.com/events'
        today = pd.Timestamp.today().normalize()
        payload = {
            'from': (today + pd.offsets.Hour(0)).isoformat() + '.000Z',
            'to': (today + pd.offsets.Day(14) + pd.offsets.Hour(22)).isoformat() + '.000Z',
            'countries': ','.join(['US'])   }
        data = requests.get(url, params=payload).json()
        with open(path, 'wb') as handle:
            pickle.dump(data, handle, protocol=pickle.HIGHEST_PROTOCOL)
    df = pd.DataFrame(data['result'])
    df = df[df['country']=='US']
    df=df[df['importance']>=1]
    df = df[['indicator','date']]
    df['date'] = df['date'].apply(lambda text: text[:10])

    return df

@st.cache_data
def getSectors(today):
    symbols = ['XLC', 'XLY', 'XLP', 'XLE', 'XLF', 'XLV', 'XLI', 'XLB', 'XLRE', 'XLK', 'XLU']
    ema, sma = [8,20,21], [50,200]
    names = {'XLC':'Communication Services', 
        'XLY':'Consumer Discretionary',
        'XLP': 'Consumer Staples', 
        'XLE': 'Energy', 
        'XLF': 'Financials', 
        'XLV': 'Health Care', 
        'XLI': 'Industrials', 
        'XLB': 'Materials', 
        'XLRE': 'Real Estate', 
        'XLK': 'Technology',
        'XLU': 'Utilities'} 
    path = './cache_csv/'+'Sectors'+today.strftime("%m-%d-%Y")+'.csv'
    check_file = os.path.isfile(path)
    if check_file:
        data=pd.read_csv(path,index_col=0)
    else:
        data = yf.download(tickers=symbols, period="2y", interval="1d")
        data = data['Close']
        data.fillna(method="ffill",inplace=True)
        for sector in symbols:
            for x in sma: data[sector+"_SMA_"+str(x)] = round( data[sector].rolling(x).mean() , 2)
            for x in ema: data[sector+"_EMA_"+str(x)] = round( data[sector].ewm(span=x).mean() , 2)
        data.to_csv(path)
    return data, symbols, names


@st.cache_data
def getSectorsTrends(today):
    sectorsTrends = {}
    for symbol in symbols:
        LTtrend, phase, _ = STPhaseIndices(data[symbol],data[symbol+'_SMA_200'],data[symbol+'_EMA_8'],data[symbol+'_EMA_21'])
        sectorsTrends[symbol] = LTtrend, phase
    return sectorsTrends



@st.cache_data
def getEarnings(today):
    
    def getdays(datestr):
        if not datestr: return -1
        if pd.isnull(datestr): return -1
        format =  '%Y-%m-%d'
        earningsdate = datetime.datetime.strptime(datestr,format)
        #current_dateTime = datetime.now()
        return (earningsdate-today).days

    path2 = './cache_csv/'+'CandEarnings'+today.strftime("%m-%d-%Y")+'.csv'
    check_file = os.path.isfile(path2)
    if check_file:
        dee = pd.read_csv(path2,index_col=0)
        return dee
    
    path = './cache_csv/'+'Earnings.csv'
    df = pd.read_csv(path,index_col=0)
    df['Days to earnings']=df['Next Earnings Date'].apply(getdays).astype(numpy.int64)

    dee = df[df['Days to earnings']<=40]
    dee = dee[dee['Days to earnings']>=5]
    dee['Sector LT'] = dee['Index'].apply(LTemoji)
    dee['Sector ST'] = dee['Index'].apply(STemoji)

    dee['Above 34EMA'] = dee['Ticker'].apply(getabove34ema)
    dee['Above 50SMA'] = dee['Ticker'].apply(getabove50sma)
    dee['Rainbow logic'] = dee['Ticker'].apply(getRainbowlogic)
    dee['< +1ATR vs 21EMA'] =  dee['Ticker'].apply(getActionzone) 
    dee['Pullback'] = '?'
    dee['Overextended'] =  dee['Ticker'].apply(getOverextended)
    dee = dee[['Ticker','Days to earnings','Next Earnings Date','Sector','Sector LT','Sector ST','Above 34EMA','Above 50SMA','Rainbow logic','< +1ATR vs 21EMA','Pullback','Overextended']] 

    dee.to_csv(path2)
    
    return dee


##########################
##########################
##########################
# Temporary

path = './cache_csv/'+'ScanData'+datetime.datetime.now().strftime("%m-%d-%Y")+'.pickle'
check_file = os.path.isfile(path)
if check_file:
    with open(path, 'rb') as handle:
        rez = pickle.load(handle)
        dt,dic_lists, dic_scaned = rez[0], rez[1], rez[2] 


##########################
##########################
##########################

# Get economic calendar
dfcal = getEcCalendar(today)

# Get sector information
data, symbols, names = getSectors(today)
sectorsTrends = getSectorsTrends(today)

# Get Earnings information
dee = getEarnings(today)

st.write(dee)

with st.expander("Economic calendar"): st.table(dfcal.head())
