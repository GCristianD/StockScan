import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy
import datetime
import os 
import time
import pickle
import yfinance as yf
import requests
import yahooquery
import talib
import matplotlib.dates as mdates

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


#################################
######## Global variables #######
#################################

start_date = datetime.datetime(2022, 1, 1, 0, 0, 0, 0)
today=datetime.datetime.now()

namesSec = {'Communication Services': 'XLC',
     'Consumer Cyclical': 'XLY',
     'Consumer Defensive': 'XLP',
     'Energy': 'XLE',
     'Financial Services': 'XLF',
     'Healthcare': 'XLV',
     'Industrials': 'XLI',
     'Basic Materials': 'XLB',
     'Real Estate': 'XLRE',
     'Technology': 'XLK',
     'Utilities': 'XLU'}

###################################
######## Functions uncached #######
###################################
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
    
    if (SeriesPrice.iloc[-1:] > Series200SMA.iloc[-1:]).iloc[0]: LTtrend='UPTREND'
    else: LTtrend='DOWNTREND'
    
    
    if Series8Ema.iloc[-1]>Series21Ema.iloc[-1]:
        if (SeriesPrice.iloc[-1]>Series21Ema.iloc[-1]): 
            if (SeriesPrice.iloc[-2]>Series21Ema.iloc[-2]): phase = 'BULL phase - Strong Uptrend'
            else: phase = 'BULL phase - Uptrend'
    else:
        if (SeriesPrice.iloc[-1]<Series21Ema.iloc[-1]):
            if (SeriesPrice.iloc[-2]<Series21Ema.iloc[-1]): phase = 'BEAR phase - Strong Downtrend'
            else: phase = 'BEAR phase - Downtrend'
            
    if LTtrend=='UPTREND':
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

def add_stochastic_oscillator(df, periods=8):
  high_roll = df["High"].rolling(periods).max()
  low_roll = df["Low"].rolling(periods).min()
  # Fast stochastic indicator
  num = df["Close"] - low_roll
  denom = high_roll - low_roll
  df["%K"] = (num / denom) * 100
  # Slow stochastic indicator
  df["%D"] = df["%K"].rolling(3).mean()

def add_rsi(df, periods = 2):
  close_delta = df['Close'].diff()
  # Make two series: one for lower closes and one for higher closes
  up = close_delta.clip(lower=0)
  down = -1 * close_delta.clip(upper=0)
  ma_up = up.ewm(com = periods - 1, adjust=True, min_periods = periods).mean()
  ma_down = down.ewm(com = periods - 1, adjust=True, min_periods = periods).mean()
  rsi = ma_up / ma_down
  rsi = 100 - (100/(1 + rsi))
  df['RSI'] = rsi

def add_ADX(df, timeperiod=13):
    df['ADX'] = talib.ADX(df['High'], df['Low'], df['Close'], timeperiod=13)

def add_MAs(df):
    ema = [8,21,34,55,89]
    for x in ema: df["EMA_"+str(x)] = round( df['Close'].ewm(span=x).mean() , 2)
    sma = [50,100,200]
    for x in sma: df["SMA_"+str(x)] = round( df['Close'].rolling(x).mean() , 2)

def add_keltner(df):
    # Compute EMA
    if 'EMA_21' not in df.keys():
        df['EMA_21'] = df['Close'].ewm(span=21, adjust=False).mean()
    # Compute ATR
    df['ATR'] = talib.ATR(df.High,df.Low,df.Close, timeperiod=21)
    # Compute Upper and Lower Bands
    df['UpperBand1'] = round(df['EMA_21'] + 1 * df['ATR'],2)
    df['LowerBand1'] = round(df['EMA_21'] - 1 * df['ATR'],2)

    df['UpperBand2'] = round(df['EMA_21'] + 2 * df['ATR'],2)
    df['LowerBand2'] = round(df['EMA_21'] - 2 * df['ATR'],2)

    df['UpperBand3'] = round(df['EMA_21'] + 3 * df['ATR'],2)
    df['LowerBand3'] = round(df['EMA_21'] - 3 * df['ATR'],2)

    #df['date'] = mdates.date2num(df.index.to_pydatetime())
    df['date'] = mdates.date2num(pd.DatetimeIndex(df.index).to_pydatetime())


#################################
######## Dispplay functions #####
#################################

def displayStockListoptions():
    c1, c2 = st.columns(2)
    with c1:
        totaloptions = st.multiselect('**Scan from:**', options=['Mega-cap','Large-cap','Mid-cap','Small-cap'],
                default=['Mega-cap','Large-cap','Mid-cap'],key='totalstocks')
    with c2:
        st.write('')
        st.write('')    
        nonUS = st.checkbox('Include non-US stocks')
    

    caps_dic, capslist = {'Mega-cap':'Mega','Large-cap':'Large','Mid-cap':'Mid','Small-cap':'Small'}, []
    for opt in ['Mega-cap','Large-cap','Mid-cap','Small-cap']:
        if opt in totaloptions:
            capslist += [caps_dic[opt]]
    return totaloptions, nonUS, capslist

def displayTrend():
    if LTtrend=='UPTREND':
        st.write(f"S&P500 (vs its 200SMA) is in a long-term: :green[{LTtrend}]. Is market overextended: {OE} ")
    else:
        st.write(f"S&P500 (vs its 200SMA) is in a long-term: :red[{LTtrend}]. Is market overextended: {OE} ")

    if phase in ['BULL phase - Strong Uptrend','BULL phase - Uptrend']:
        st.write(f"Short-term (8 vs 21 EMAs): :green[{phase}].  "+f' The focus is: :green[{focus}]')
    elif phase in ['BEAR phase - Strong Downtrend', 'BEAR phase - Downtrend']:
        st.write(f"Short-term (8 vs 21 EMAs): :red[{phase}]"+f' The focus is: :red[{focus}]')
    else: 
        st.write(f"Short-term (8 vs 21 EMAs): {phase}"+f' The focus is: {focus}')


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
def getSP500(today):    
    path = './cache_csv/'+'Mkt'+today.strftime("%m-%d-%Y")+'.csv'
    check_file = os.path.isfile(path)
    if check_file:
        df=pd.read_csv(path,index_col=0)
    else: 
        df = yf.download('^GSPC', start_date, today)
        add_stochastic_oscillator(df)
        add_rsi(df)
        add_ADX(df)
        add_MAs(df)
        add_keltner(df)
        df.reset_index(inplace=True)
        
        df.to_csv(path)
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



#@st.cache_data
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
        lse = list(dee['Ticker'])
        return dee, lse
    
    path = './ListOfStocks/'+'Earnings.csv'
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

    lse = list(dee['Ticker'])
    return dee, lse

@st.cache_data
def getTrendPhaseFocus(today):
    LTtrend, phase, focus  =  STPhaseIndices(dm['Close'],dm['SMA_200'],dm['EMA_8'],dm['EMA_21'])  
    return LTtrend, phase, focus, dm['Close'].iloc[-1] >= dm['UpperBand2'].iloc[-1]

@st.cache_data
def get_earn(stock):
        rez = yahooquery.Ticker(stock).calendar_events[stock]['earnings']['earningsDate']
        if not rez: return '-'
        rez = rez[0].split(' ')[0]
        return rez[5:]  # i.e. don't return year    

@st.cache_data
def createScanTables(today,capslist,nonUS):
    path = './cache_csv/'+'CandInfoTot'+today.strftime("%m-%d-%Y")+'.csv'
    check_file = os.path.isfile(path)
    if check_file:
        dff = pd.read_csv(path,index_col=0)
    else:   
        df = pd.read_csv('./ListOfStocks/'+'AllProcessed.csv', index_col=0)
        list_scanned = dic_lists['bull']+dic_lists['bull_rsi']+dic_lists['bull_vol']+dic_lists['bear']+dic_lists['bear_rsi']+dic_lists['bear_vol'] 
        dff = df[df['Symbol'].isin(list_scanned)]
        dff['Bull Cost cond.'] = dff['Symbol'].apply(lambda stock: '✔' if stock in dic_lists['bull'] else ' ')
        dff['Bullish RSI'] = dff['Symbol'].apply(lambda stock: '✔' if stock in dic_lists['bull_rsi'] else ' ')
        dff['Bear Cost cond.'] = dff['Symbol'].apply(lambda stock: '✔' if stock in dic_lists['bear'] else ' ')
        dff['Bearish RSI'] = dff['Symbol'].apply(lambda stock: '✔' if stock in dic_lists['bear_rsi'] else ' ')

        dff['Earnings']=dff['Symbol'].apply(get_earn)
        dff['Index']=dff['Sector'].apply(lambda x: namesSec[x])
        dff['Sector LT'] = dff['Index'].apply(lambda symbol: sectorsTrends[symbol][0])
        dff['Sector ST'] = dff['Index'].apply(lambda symbol: sectorsTrends[symbol][1])
        dff['Sector LT'] = dff['Index'].apply(LTemoji)
        dff['Sector ST'] = dff['Index'].apply(STemoji)
        dff['Company Name'] = dff['Company Name'].apply(lambda name: name[:20])
        dff.to_csv(path)
    
    dff = dff[dff['Cap'].isin(capslist)]
    if not nonUS: dff = dff[dff['Loc'] == 'US']
    return dff

@st.cache_data
def make_charts(df, ticker):
    fig1 = go.Figure(data=[go.Candlestick(x=df['Date'],
            open=df['Open'],
            high=df['High'],
            low=df['Low'],
            close=df['Close'])
            ,go.Scatter(x=df.Date, y=df['EMA_8'], line=dict(color='red', width=1), name="EMA8")               
            ,go.Scatter(x=df.Date, y=df['EMA_21'], line=dict(color='orange', width=1), name="EMA21")               
            ,go.Scatter(x=df.Date, y=df['EMA_34'], line=dict(color='yellow', width=1), name="EMA34")               
                          
            ,go.Scatter(x=df.Date, y=df['SMA_50'], line=dict(color='green', width=1), name="SMA50")               
            ,go.Scatter(x=df.Date, y=df['SMA_100'], line=dict(color='blue', width=1), name="SMA100")               
            ,go.Scatter(x=df.Date, y=df['SMA_200'], line=dict(color='violet', width=2), name="SMA200")            
            ])
    fig1.update_layout(height=800, title=ticker)

    fig2=go.Figure(data=[go.Scatter(
         x=df.Date,
         y=df["%K"], line=dict(color='blue', width=2), name="%K (Fast)"), 
         go.Scatter(
         x=df.Date,
         y=df["%D"], line=dict(color='orange', width=2), name="%D (Slow)")               
                        ])
    fig2.add_hline(y=60, line_width=2, line_dash="dash", line_color="red")
    fig2.add_hline(y=40, line_width=2, line_dash="dash", line_color="green")
    fig2.update_layout(height=400)

    fig3=go.Figure(data=[go.Scatter(
         x=df.Date,
         y=df["RSI"], line=dict(color='blue', width=2), name="RSI")               ])
    fig3.add_hline(y=90, line_width=2, line_dash="dash", line_color="red", name='90')
    fig3.add_hline(y=10, line_width=2, line_dash="dash", line_color="green", name='10')
    fig3.update_layout(height=400)

    fig4 = go.Figure(data=[go.Candlestick(x=df['Date'],
            open=df['Open'],
            high=df['High'],
            low=df['Low'],
            close=df['Close'])              
            ,go.Scatter(x=df.Date, y=df['EMA_21'], line=dict(color='gray', width=1), name="EMA21")
            ,go.Scatter(x=df.Date, y=df['LowerBand1'], line=dict(color='violet', width=1), name="Lower Band") 
            ,go.Scatter(x=df.Date, y=df['UpperBand1'], line=dict(color='violet', width=1), name="Upper Band")               
                          
            ,go.Scatter(x=df.Date, y=df['UpperBand2'], line=dict(color='cyan', width=1), name="Upper 2 Band")               
            ,go.Scatter(x=df.Date, y=df['LowerBand2'], line=dict(color='cyan', width=1), name="Lower 2 Band")               
            ,go.Scatter(x=df.Date, y=df['UpperBand3'], line=dict(color='red', width=2), name="Upper 3 Band")
            ,go.Scatter(x=df.Date, y=df['LowerBand3'], line=dict(color='green', width=2), name="Lower 3 Band") 
            ])
    fig4.update_layout(height=800)

    return fig1, fig2, fig3, fig4


#Do not cache
def display_charts(scanlist,i):
    if scanlist:
        totaloptions = st.multiselect('**Plot stocks:**', options=scanlist, default=scanlist, key='plotstocks'+i*' ')

        c1, c2, c3 = st.columns(3)
        with c1: stochindcb = st.checkbox('Show Stochastic indicator'+i*' ')
        with c2: rsicb = st.checkbox('Show RSI'+i*' ')
        with c3: keltnercb = st.checkbox('Show Keltner channels'+i*' ')

        for ticker in totaloptions:
            df = dic_scaned[ticker]
            df.reset_index(inplace=True)

            fig1, fig2, fig3, fig4 = make_charts(df,ticker)

            #st.button(ticker + " - Add to watchlist")
                
            st.plotly_chart(fig1,theme=None, use_container_width=True)

            if stochindcb: st.plotly_chart(fig2,theme=None, use_container_width=True)
            if rsicb: st.plotly_chart(fig3,theme=None, use_container_width=True)
            if keltnercb: st.plotly_chart(fig4,theme=None, use_container_width=True)



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
dee, lse = getEarnings(today)

#Get S&P500, market trend and focus
dm = getSP500(today)
LTtrend, phase, focus, OE = getTrendPhaseFocus(today)

# Display Market trend
displayTrend()

#Display Stock Universe options
totaloptions, nonUS, capslist = displayStockListoptions()



#Get scan results tables
dff = createScanTables(today,capslist,nonUS)



# Display


Bullishscan, Bearishscan, Earningsscan = st.tabs(['Bullish scan', 'Bearish scan', 'Earnings scan'])

with Bullishscan:
    st.table(dff[dff['Symbol'].isin(dic_lists['bull']+dic_lists['bull_rsi'])][['Symbol','Bull Cost cond.','Bullish RSI','Earnings','Cap','Loc','Sector','Sector LT','Sector ST']])

    chartlistbullnow = list(dff[dff['Symbol'].isin(dic_lists['bull']+dic_lists['bull_rsi'])]['Symbol'])
    display_charts(chartlistbullnow,0)
    
with Bearishscan:

    st.table(dff[dff['Symbol'].isin(dic_lists['bear']+dic_lists['bear_rsi'])][['Symbol','Bear Cost cond.','Bearish RSI','Earnings','Cap','Loc','Sector','Sector LT','Sector ST']])


with Earningsscan:
    st.write("Earnings soon:")
    st.table(dee)
    display_charts(lse,4)

