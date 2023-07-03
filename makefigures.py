import pandas as pd
import numpy
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots



#################################
#################################
#################################

def make_charts(df, ticker):
    fig = make_subplots(rows=4, cols=1, vertical_spacing=0.012, subplot_titles=(ticker,"Stochastic Oscillator","RSI","Kelner channels"), shared_xaxes=True, row_heights=[0.6, 0.1,0.1,0.6])

    fig.add_trace(go.Candlestick(x=df['Date'],
                open=df['Open'],
                high=df['High'],
                low=df['Low'],
                close=df['Close'], name='Rainbow') ,row=1, col=1)
    fig.add_trace(go.Scatter(x=df.Date, y=df['EMA_8'], line=dict(color='red', width=1), name="EMA8") , row=1, col=1)
    fig.add_trace(go.Scatter(x=df.Date, y=df['EMA_21'], line=dict(color='orange', width=1), name="EMA21"), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.Date, y=df['EMA_34'], line=dict(color='yellow', width=1), name="EMA34") , row=1, col=1)
    fig.add_trace(go.Scatter(x=df.Date, y=df['SMA_50'], line=dict(color='green', width=1), name="SMA50"), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.Date, y=df['SMA_100'], line=dict(color='blue', width=1), name="SMA100"), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.Date, y=df['SMA_200'], line=dict(color='violet', width=2), name="SMA200"), row=1, col=1)


    fig.add_trace(go.Scatter(
             x=df.Date,
             y=df["%D"], line=dict(color='orange', width=2), name="%K (Slow)"), row=2, col=1)
    fig.add_hline(y=60, line_width=2, line_dash="dash", line_color="red",row=2, col=1)
    fig.add_hline(y=40, line_width=2, line_dash="dash", line_color="green",row=2, col=1)


    fig.add_trace(go.Scatter(
         x=df.Date,
         y=df["RSI"], line=dict(color='gray', width=2), name="RSI"), row=3, col=1)
    fig.add_hline(y=90, line_width=2, line_dash="dash", line_color="red", name='90', row=3, col=1)
    fig.add_hline(y=10, line_width=2, line_dash="dash", line_color="green", name='10', row=3, col=1)


    fig.add_trace(go.Candlestick(x=df['Date'],
                open=df['Open'],
                high=df['High'],
                low=df['Low'],
                close=df['Close'], name='Kelner channels'), row=4, col=1)
    fig.add_trace(go.Scatter(x=df.Date, y=df['EMA_21'], line=dict(color='gray', width=1), name="EMA21") , row=4, col=1)
    fig.add_trace(go.Scatter(x=df.Date, y=df['LowerBand1'], line=dict(color='violet', width=1), name="Lower Band"), row=4, col=1)
    fig.add_trace(go.Scatter(x=df.Date, y=df['UpperBand1'], line=dict(color='violet', width=1), name="Upper Band"), row=4, col=1)

    fig.add_trace(go.Scatter(x=df.Date, y=df['UpperBand2'], line=dict(color='cyan', width=1), name="Upper 2 Band"), row=4, col=1)
    fig.add_trace(go.Scatter(x=df.Date, y=df['LowerBand2'], line=dict(color='cyan', width=1), name="Lower 2 Band"), row=4, col=1)
    fig.add_trace(go.Scatter(x=df.Date, y=df['UpperBand3'], line=dict(color='red', width=2), name="Upper 3 Band"), row=4, col=1)
    fig.add_trace(go.Scatter(x=df.Date, y=df['LowerBand3'], line=dict(color='green', width=2), name="Lower 3 Band"), row=4, col=1)


    fig.update_layout(height=1600) 

    #fig.update_layout(
    #    title_text=ticker)

    fig.update_layout(
        xaxis=dict(
            rangeselector=dict(
                buttons=[
                    dict(count=6,
                         label="6m",
                         step="month",
                         stepmode="backward"),
                    dict(count=1,
                         label="YTD",
                         step="year",
                         stepmode="todate"),
                    dict(count=1,
                         label="1y",
                         step="year",
                         stepmode="backward"),
                    dict(step="all")
                ]),
            type="date"),#end xaxis  definition

        xaxis1_rangeslider_visible=False,
        xaxis2_rangeslider_visible=False,
        xaxis3_rangeslider_visible=False,
        xaxis4_rangeslider_visible=False,
        xaxis4_type="date"
        );

    return fig

def makefig_squeeze(df,ticker):    
    fig = make_subplots(rows=4, cols=1, vertical_spacing=0.005, shared_xaxes=True, row_heights=[0.6, 0.1,0.1,0.6])

    fig.add_trace(go.Candlestick(x=df['Date'],
                open=df['Open'],
                high=df['High'],
                low=df['Low'],
                close=df['Close'], name=ticker))
    fig.add_trace(go.Scatter(x=df.Date, y=df['EMA_8'], line=dict(color='red', width=1), name="EMA8") , row=1, col=1)
    fig.add_trace(go.Scatter(x=df.Date, y=df['EMA_21'], line=dict(color='orange', width=1), name="EMA21"), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.Date, y=df['EMA_34'], line=dict(color='yellow', width=1), name="EMA34") , row=1, col=1)
    fig.add_trace(go.Scatter(x=df.Date, y=df['SMA_50'], line=dict(color='green', width=1), name="SMA50"), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.Date, y=df['SMA_100'], line=dict(color='blue', width=1), name="SMA100"), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.Date, y=df['SMA_200'], line=dict(color='violet', width=2), name="SMA200"), row=1, col=1)

    fig.add_trace(go.Scatter(x=df[df['Squeeze']==1]['Date'],y=df[df['Squeeze']==1]['Squeeze'].astype(int)-1  ,mode="markers", marker_color='red') , row=2, col=1)

    customscale = [[0, "coral"], [1, "green"]]
        
    fig.add_trace(go.Bar(x=df['Date'], y=df['value'],marker=dict(color = df['color'], colorscale=customscale))  , row=2, col=1)
    
    
    fig.add_trace(go.Scatter(
             x=df.Date,
             y=df["%D"], line=dict(color='orange', width=2), name="%K (Slow)"), row=3, col=1)
    fig.add_hline(y=60, line_width=2, line_dash="dash", line_color="red",row=3, col=1)
    fig.add_hline(y=40, line_width=2, line_dash="dash", line_color="green",row=3, col=1)

    fig.add_trace(go.Candlestick(x=df['Date'],
                open=df['Open'],
                high=df['High'],
                low=df['Low'],
                close=df['Close'], name='Kelner channels'), row=4, col=1)
    fig.add_trace(go.Scatter(x=df.Date, y=df['EMA_21'], line=dict(color='gray', width=1), name="EMA21") , row=4, col=1)
    fig.add_trace(go.Scatter(x=df.Date, y=df['LowerBand1'], line=dict(color='violet', width=1), name="Lower Band"), row=4, col=1)
    fig.add_trace(go.Scatter(x=df.Date, y=df['UpperBand1'], line=dict(color='violet', width=1), name="Upper Band"), row=4, col=1)
    fig.add_trace(go.Scatter(x=df.Date, y=df['UpperBand2'], line=dict(color='cyan', width=1), name="Upper 2 Band"), row=4, col=1)
    fig.add_trace(go.Scatter(x=df.Date, y=df['LowerBand2'], line=dict(color='cyan', width=1), name="Lower 2 Band"), row=4, col=1)
    fig.add_trace(go.Scatter(x=df.Date, y=df['UpperBand3'], line=dict(color='red', width=2), name="Upper 3 Band"), row=4, col=1)
    fig.add_trace(go.Scatter(x=df.Date, y=df['LowerBand3'], line=dict(color='green', width=2), name="Lower 3 Band"), row=4, col=1)

    
    fig.update_layout(height=1600) 

    fig.update_layout(
        title_text=ticker)

    fig.update_layout(
        xaxis=dict(
            rangeselector=dict(
                buttons=[
                    dict(count=6,
                         label="6m",
                         step="month",
                         stepmode="backward"),
                    dict(count=1,
                         label="YTD",
                         step="year",
                         stepmode="todate"),
                    dict(count=1,
                         label="1y",
                         step="year",
                         stepmode="backward"),
                    dict(step="all")
                ]),
            type="date"),#end xaxis  definition

        xaxis1_rangeslider_visible=False,
        xaxis2_rangeslider_visible=False,
        xaxis3_rangeslider_visible=False,
        xaxis4_rangeslider_visible=False,
        xaxis4_type="date"
        );
    
    return fig
