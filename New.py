import streamlit as st
import pandas as pd
import numpy
from datetime import datetime
import pickle
from makefigures import make_charts, makefig_squeeze

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

st.markdown("""
        <style>
               .block-container {
                    padding-top: 1rem;
                    padding-bottom: 0rem;
                    padding-left: 5rem;
                    padding-right: 5rem;
                }
        </style>
        """, unsafe_allow_html=True)

col001, col002 = st.columns(2)

with col001:
    st.title('📊 Daily Stock Scanner')

with col002:
    st.caption('*Project based on [The Tao of Trading](https://www.amazon.com/Tao-Trading-Abundant-Wealth-Condition/dp/1544508166) and the [Options Academy](https://www.taooftrading.com/options-academy?r_done=1) by Simon Ree*')
    st.caption('*This page is for educational and demonstation purposes only*')

#################################
######## Dispplay functions #####
#################################

def displayStockListoptions(lastday):
    c1, c2 = st.columns(2)
    with c1:
        totaloptions = st.multiselect('**Scan from:**', options=['Mega-cap','Large-cap','Mid-cap','Small-cap'],
                default=['Mega-cap','Large-cap','Mid-cap'],key='totalstocks')
    with c2:
        c21, c22 = st.columns(2)
        with c21:
            st.write('')
            st.write('')    
            nonUS = st.checkbox('Include non-US stocks', value=True)
        with c22:
            st.write('')
            st.write('')
            st.write('Last trading day data: '+ lastday)

    caps_dic, capslist = {'Mega-cap':'Mega','Large-cap':'Large','Mid-cap':'Mid','Small-cap':'Small'}, []
    for opt in ['Mega-cap','Large-cap','Mid-cap','Small-cap']:
        if opt in totaloptions:
            capslist += [caps_dic[opt]]
    return totaloptions, nonUS, capslist




#################################
#################################
#################################

@st.cache_data
def loadPrices():
    path = 'scanned.pickle'
    with open(path, 'rb') as handle:
        dic_scaned = pickle.load(handle)
    return dic_scaned


@st.cache_data
def createTables():
    path = 'tables.pickle'
    with open(path, 'rb') as handle:
        rez = pickle.load(handle)

    return rez[0], rez[1], rez[2], rez[3],  rez[4], rez[5], rez[6], rez[7], rez[8], rez[9], rez[10], rez[11], rez[12], rez[13], rez[14], rez[15]


#################################
#################################
#################################

# Load price data for the creation of the figures
dic_scaned = loadPrices()


#Load tables
dbear, dbull, dbull200, dbear200,   dsqfilt2_bull, dsqfilt3_bull, dsqfilt_bear, \
d_bullsetup_bulltrend_conservative, d_bullsetup_beartrend_conservative, d_bearsetup_bulltrend_conservative, d_bearsetup_beartrend_conservative, \
d_bullsetup_bulltrend_aggresive, d_bullsetup_beartrend_aggresive, d_bearsetup_bulltrend_aggresive, d_bearsetup_beartrend_aggresive, lastday = createTables()


#Display Stock Universe options
totaloptions, nonUS, capslist = displayStockListoptions(lastday)


#Restrict tables:
dfbull = dbull[dbull['Cap'].isin(capslist)]
dfbear = dbear[dbear['Cap'].isin(capslist)]

dsqfilt_bear = dsqfilt_bear[dsqfilt_bear['Cap'].isin(capslist)]

dsqfilt2_bull = dsqfilt2_bull[dsqfilt2_bull['Cap'].isin(capslist)]
dsqfilt3_bull = dsqfilt3_bull[dsqfilt3_bull['Cap'].isin(capslist)]

d_bullsetup_bulltrend_conservative = d_bullsetup_bulltrend_conservative[d_bullsetup_bulltrend_conservative['Cap'].isin(capslist)]
d_bullsetup_beartrend_conservative = d_bullsetup_beartrend_conservative[d_bullsetup_beartrend_conservative['Cap'].isin(capslist)]
d_bearsetup_bulltrend_conservative = d_bearsetup_bulltrend_conservative[d_bearsetup_bulltrend_conservative['Cap'].isin(capslist)]
d_bearsetup_beartrend_conservative = d_bearsetup_beartrend_conservative[d_bearsetup_beartrend_conservative['Cap'].isin(capslist)]
d_bullsetup_bulltrend_aggresive = d_bullsetup_bulltrend_aggresive[d_bullsetup_bulltrend_aggresive['Cap'].isin(capslist)]
d_bullsetup_beartrend_aggresive = d_bullsetup_beartrend_aggresive[d_bullsetup_beartrend_aggresive['Cap'].isin(capslist)]
d_bearsetup_bulltrend_aggresive = d_bearsetup_bulltrend_aggresive[d_bearsetup_bulltrend_aggresive['Cap'].isin(capslist)]
d_bearsetup_beartrend_aggresive = d_bearsetup_beartrend_aggresive[d_bearsetup_beartrend_aggresive['Cap'].isin(capslist)]

dbull200 = dbull200[dbull200['Cap'].isin(capslist)]
dbear200 = dbear200[dbear200['Cap'].isin(capslist)]

if not nonUS:
    dfbull = dfbull[dfbull['Loc'] == 'US']
    dfbear = dfbear[dfbear['Loc'] == 'US']
    dsqfilt_bear = dsqfilt_bear[dsqfilt_bear['Loc'] == 'US']
    dsqfilt2_bull = dsqfilt2_bull[dsqfilt2_bull['Loc'] == 'US']
    dsqfilt3_bull = dsqfilt3_bull[dsqfilt3_bull['Loc'] == 'US']

    d_bullsetup_bulltrend_conservative = d_bullsetup_bulltrend_conservative[d_bullsetup_bulltrend_conservative['Loc'] == 'US']
    d_bullsetup_beartrend_conservative = d_bullsetup_beartrend_conservative[d_bullsetup_beartrend_conservative['Loc'] == 'US']
    d_bearsetup_bulltrend_conservative = d_bearsetup_bulltrend_conservative[d_bearsetup_bulltrend_conservative['Loc'] == 'US']
    d_bearsetup_beartrend_conservative = d_bearsetup_beartrend_conservative[d_bearsetup_beartrend_conservative['Loc'] == 'US']
    d_bullsetup_bulltrend_aggresive = d_bullsetup_bulltrend_aggresive[d_bullsetup_bulltrend_aggresive['Loc'] == 'US']
    d_bullsetup_beartrend_aggresive = d_bullsetup_beartrend_aggresive[d_bullsetup_beartrend_aggresive['Loc'] == 'US']
    d_bearsetup_bulltrend_aggresive = d_bearsetup_bulltrend_aggresive[d_bearsetup_bulltrend_aggresive['Loc'] == 'US']
    d_bearsetup_beartrend_aggresive = d_bearsetup_beartrend_aggresive[d_bearsetup_beartrend_aggresive['Loc'] == 'US']

    dbull200 = dbull200[dbull200['Loc'] == 'US']
    dbear200 = dbear200[dbear200['Loc'] == 'US']



##path = 'figs.pickle'
##with open(path, 'rb') as handle:
##    rez = pickle.load(handle)
##    figs_bounce,figs_squeeze = rez[0], rez[1]


# Make tabs
BounceScan, Bounce200,  Earningsscan, SqueezeScan, CountertrendScan  = st.tabs(['Bounce scan','Bounce 200', 'Earnings', 'Squeezes', 'Countertrend scan'])

with BounceScan:
    Bullishscan, Bearishscan = st.tabs(['Bullish scan','Bearish scan'])
    with Bullishscan:
        st.caption('8 EMA > 21 EMA > 34 EMA > 55 EMA > 89 EMA, \u2001 ADX(13) > 19, \u2001 Stochastics %K (8,3) ≤ 40. \u2001 \u2001 \u2001 Bullish RSI: prev. RSI(2) < 10 < RSI(2), \u2001 Bull Cost cond: Close > Open and Close > prev. bar High')
        st.table(dfbull[['Symbol','Bull Cost cond.','Bullish RSI','In Squeeze','ATRs vs mean','% of 52w high', 'Bull Rainow %',
                         'Cap','Sector','Sector LT','Sector ST','Loc']].style.format({'ATRs vs mean': "{:.2f}",'Bull Rainow %': "{:.0f}", 'Bull Rainow All %': "{:.2f}" }))
        lsbull = list(dfbull['Symbol'])
        if lsbull: Bullfigtabs = st.tabs(lsbull)
        for i,ticker in enumerate(lsbull):
            with Bullfigtabs[i]:
                ticker = lsbull[i]
                st.write(dfbull.iloc[i]['Company Name'])
                df = dic_scaned[ticker]
                fig = make_charts(df,ticker)
                #fig = figs_bounce[ticker]
                st.plotly_chart(fig,theme=None, use_container_width=True)

        
    with Bearishscan:
        st.caption('8 EMA < 21 EMA < 34 EMA < 55 EMA < 89 EMA, \u2001 ADX(13) > 19, \u2001 Stochastics %K (8,3) ≥ 60. \u2001 \u2001 \u2001 Bearish RSI: prev. RSI(2) > 90 > RSI(2), \u2001 Bear Cost cond: Close < Open and Close < prev. bar Low')

        st.table(dfbear[['Symbol','Bear Cost cond.','Bearish RSI','In Squeeze','ATRs vs mean','% of 52w high','Bear Rainow %',
                         'Cap','Sector','Sector LT','Sector ST','Loc']].style.format({'ATRs vs mean': "{:.2f}", 'Bear Rainow %': "{:.0f}"} ))

        lsbear = list(dfbear['Symbol'])

        if lsbear: Bearfigtabs = st.tabs(lsbear)
        else: lsbear = []
        
        for i,ticker in enumerate(lsbear):
            with Bearfigtabs[i]:
                
                ticker = lsbear[i]
                st.write(dfbear.iloc[i]['Company Name'])
                df = dic_scaned[ticker]
                fig = make_charts(df,ticker) 
                #fig = dic_figs[ticker]
                st.plotly_chart(fig,theme=None, use_container_width=True)

with Bounce200:
    Bullish200, Bearish200 = st.tabs(['Bullish Bounce 200','Bearish Bounce 200'])
    with Bullish200:
        st.caption('High > 200 SMA > Low, \u2001 50 SMA > 200 SMA, \u2001 Stochastics %K (8,3) ≤ 40')
        st.table(dbull200[['Symbol','In Squeeze','ATRs vs mean','% of 52w high', 'Bull Rainow %',
                         'Cap','Sector','Sector LT','Sector ST','Loc']].style.format({'ATRs vs mean': "{:.2f}",'Bull Rainow %': "{:.0f}", 'Bull Rainow All %': "{:.1f}" }))

        lsbull200 = list(dbull200['Symbol'])
        if lsbull200: Bull200figtabs = st.tabs(lsbull200)
        for i,ticker in enumerate(lsbull200):
            with Bull200figtabs[i]:
                st.write(dbull200.iloc[i]['Company Name'])
                ticker = lsbull200[i]
                df = dic_scaned[ticker]
                fig = make_charts(df,ticker) 
                st.plotly_chart(fig,theme=None, use_container_width=True)


    with Bearish200:
        st.caption('High > 200 SMA > Low, \u2001 50 SMA < 200 SMA, \u2001 Stochastics %K (8,3) ≥ 60')
        st.table(dbear200[['Symbol','In Squeeze','ATRs vs mean','% of 52w high', 'Bear Rainow %',
                           'Cap','Sector','Sector LT','Sector ST','Loc']].style.format({'ATRs vs mean': "{:.2f}",'Bear Rainow %': "{:.0f}" }))

        lsbear200 = list(dbear200['Symbol'])
        if lsbear200: Bear200figtabs = st.tabs(lsbear200)
        for i,ticker in enumerate(lsbear200):
            with Bear200figtabs[i]:
                st.write(dbear200.iloc[i]['Company Name'])
                ticker = lsbear200[i]
                df = dic_scaned[ticker]
                fig = make_charts(df,ticker) 
                st.plotly_chart(fig,theme=None, use_container_width=True)

                                      
with SqueezeScan:
    Bullishsqueeze, Bearishsqueeze = st.tabs(['Bullish squeeze','Bearish squeeze'])
    with Bullishsqueeze:
        st.caption('In squeeze, \u2001 8 EMA > 34 EMA, \u2001 Price within 10% of 52w high, \u2001 Squeeze momentum histogram > was 2 bars ago \u2001 and weekly: ADX ≥ 20, DI+ > DI- ')
        st.table(dsqfilt2_bull[['Symbol','Squeeze days','Wkl ADX','Wkl DI±','Wkl 10S> 34E','Wkl sq fired','ATRs vs mean','% of 52w high','Bull Rainow %',  #'Days to earnings',
                                'Cap','Sector','Sector LT','Sector ST','Loc']].style.format({'ATRs vs mean': "{:.2f}",'Bull Rainow %': "{:.2f}", 'Bull Rainow All %': "{:.2f}" }))
        st.table(dsqfilt3_bull[['Symbol','Squeeze days','Wkl ADX','Wkl DI±','Wkl 10S> 34E','Wkl sq fired','ATRs vs mean','% of 52w high','Bull Rainow %', # 'Days to earnings',
                                'Cap','Sector','Sector LT','Sector ST','Loc']].style.format({'ATRs vs mean': "{:.2f}",'Bull Rainow %': "{:.0f}", 'Bull Rainow All %': "{:.2f}" }))

        lsSqbull = list(dsqfilt2_bull['Symbol'])+ list(dsqfilt3_bull['Symbol'])
        if lsSqbull: BullSqueezefigtabs = st.tabs(lsSqbull)

        for i,ticker in enumerate(lsSqbull):
            with BullSqueezefigtabs[i]:
                ticker = lsSqbull[i]
                #st.write(dsqfilt2_bull.iloc[i]['Company Name'])
                df = dic_scaned[ticker]
                fig = makefig_squeeze(df,ticker) 
                st.plotly_chart(fig,theme=None, use_container_width=True)
        
    

    with Bearishsqueeze:
        st.caption('In squeeze, \u2001 8 EMA < 34 EMA < 200 SMA, \u2001 Squeeze momentum histogram <0 \u2001 and weekly: ADX ≥ 20, DI+ < DI- ')
        st.table(dsqfilt_bear[['Symbol','Squeeze days','ATRs vs mean','% of 52w high', 'Bear Rainow %',  #'Days to earnings',
                               'Cap','Sector','Sector LT','Sector ST','Loc']].style.format({'ATRs vs mean': "{:.2f}",'Bear Rainow %': "{:.0f}" }))

        lsSqbear = list(dsqfilt_bear['Symbol'])
        if lsSqbear: BearSqueezefigtabs = st.tabs(lsSqbear)
        
        for i,ticker in enumerate(lsSqbear):
            with BearSqueezefigtabs[i]:
                st.write(dsqfilt_bear.iloc[i]['Company Name'])
                ticker = lsSqbear[i]
                df = dic_scaned[ticker]
                fig = makefig_squeeze(df,ticker) 
                st.plotly_chart(fig,theme=None, use_container_width=True)




with CountertrendScan:
    BullishConservative,   BullishAggresive, BearishConservative,   BearishAggresive = st.tabs(['Bullish Conservative','Bullish Aggresive', 'Bearish Conservative','Bearish Aggresive'])

    with BullishConservative:
        col01, col02 = st.columns(2)
        with col01:
            st.caption('**Bull trend:**   Price > 200 SMA')
            st.caption('New 21-day low on second-last bar. Last close > previous day high')
            st.table(d_bullsetup_bulltrend_conservative[['Symbol','Cap','Sector','Loc']])
        with col02:
            st.caption('**Bear trend:**   Price < 200 SMA. Wilder RSI(13) crossed > 30 from below on last bar')
            st.caption('New 21-day low on second-last bar. Last close > previous day high')
            st.table(d_bullsetup_beartrend_conservative[['Symbol','Cap','Sector','Loc']])

    with BullishAggresive:
        col11, col12 = st.columns(2)
        with col11:
            st.caption('**Bull trend:**   Price > 200 SMA. New 21-day low')
            st.table(d_bullsetup_bulltrend_aggresive[['Symbol','Cap','Sector','Loc']])
        with col12:
            st.caption('**Bear trend:**   Price < 200 SMA. New 21-day low. Wilder RSI(13) < 30')
            st.table(d_bullsetup_beartrend_aggresive[['Symbol','Cap','Sector','Loc']])

    with BearishConservative:
        cl01, cl02 = st.columns(2)
        with cl01:
            st.caption('**Bull trend:**   Price > 200 SMA. Wilder RSI(13) crossing < 70 from above on last bar')
            st.caption('New 21-day high on second-last bar. Last close < previous day low')
            st.table(d_bearsetup_bulltrend_conservative[['Symbol','Cap','Sector','Loc']])
        with cl02:
            st.caption('**Bear trend:**   Price < 200 SMA')
            st.caption('New 21-day high on second-last bar. Last close < previous day low')
            st.table(d_bearsetup_beartrend_conservative[['Symbol','Cap','Sector','Loc']])
        

    with BearishAggresive:
        cl11, cl12 = st.columns(2)
        with cl11:
            st.caption('**Bull trend:**   Price > 200 SMA')
            st.caption('New 21-day high. Wilder RSI(13) > 70')
            st.table(d_bearsetup_bulltrend_aggresive[['Symbol','Cap','Sector','Loc']])
        with cl12:
            st.caption('**Bear trend:**   Price < 200 SMA')
            st.caption('New 21-day high. Wilder RSI(13) > 70')
            st.table(d_bearsetup_beartrend_aggresive[['Symbol','Cap','Sector','Loc']])
        
    
