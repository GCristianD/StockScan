import pandas as pd
import numpy
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots


#################################
#################################
#################################


def make_charts(df, ticker):
    fig = make_subplots(
        rows=4,
        cols=1,
        vertical_spacing=0.012,
        subplot_titles=(ticker, "Stochastic Oscillator", "RSI", "Kelner channels"),
        shared_xaxes=True,
        row_heights=[0.6, 0.1, 0.1, 0.6],
    )

    fig.add_trace(
        go.Candlestick(
            x=df["Date"],
            open=df["Open"],
            high=df["High"],
            low=df["Low"],
            close=df["Close"],
            name="Rainbow",
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df.Date, y=df["EMA_8"], line=dict(color="red", width=1), name="EMA8"
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df.Date, y=df["EMA_21"], line=dict(color="orange", width=1), name="EMA21"
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df.Date, y=df["EMA_34"], line=dict(color="yellow", width=1), name="EMA34"
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df.Date, y=df["SMA_50"], line=dict(color="green", width=1), name="SMA50"
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df.Date, y=df["SMA_100"], line=dict(color="blue", width=1), name="SMA100"
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df.Date,
            y=df["SMA_200"],
            line=dict(color="violet", width=2),
            name="SMA200",
        ),
        row=1,
        col=1,
    )

    fig.add_trace(
        go.Scatter(
            x=df.Date, y=df["%D"], line=dict(color="orange", width=2), name="%K (Slow)"
        ),
        row=2,
        col=1,
    )
    fig.add_hline(y=60, line_width=2, line_dash="dash", line_color="red", row=2, col=1)
    fig.add_hline(
        y=40, line_width=2, line_dash="dash", line_color="green", row=2, col=1
    )

    fig.add_trace(
        go.Scatter(
            x=df.Date, y=df["RSI"], line=dict(color="gray", width=2), name="RSI"
        ),
        row=3,
        col=1,
    )
    fig.add_hline(
        y=90, line_width=2, line_dash="dash", line_color="red", name="90", row=3, col=1
    )
    fig.add_hline(
        y=10,
        line_width=2,
        line_dash="dash",
        line_color="green",
        name="10",
        row=3,
        col=1,
    )

    fig.add_trace(
        go.Candlestick(
            x=df["Date"],
            open=df["Open"],
            high=df["High"],
            low=df["Low"],
            close=df["Close"],
            name="Kelner channels",
        ),
        row=4,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df.Date, y=df["EMA_21"], line=dict(color="gray", width=1), name="EMA21"
        ),
        row=4,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df.Date,
            y=df["LowerBand1"],
            line=dict(color="violet", width=1),
            name="Lower Band",
        ),
        row=4,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df.Date,
            y=df["UpperBand1"],
            line=dict(color="violet", width=1),
            name="Upper Band",
        ),
        row=4,
        col=1,
    )

    fig.add_trace(
        go.Scatter(
            x=df.Date,
            y=df["UpperBand2"],
            line=dict(color="cyan", width=1),
            name="Upper 2 Band",
        ),
        row=4,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df.Date,
            y=df["LowerBand2"],
            line=dict(color="cyan", width=1),
            name="Lower 2 Band",
        ),
        row=4,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df.Date,
            y=df["UpperBand3"],
            line=dict(color="red", width=2),
            name="Upper 3 Band",
        ),
        row=4,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df.Date,
            y=df["LowerBand3"],
            line=dict(color="green", width=2),
            name="Lower 3 Band",
        ),
        row=4,
        col=1,
    )

    fig.update_layout(height=1600)

    fig.update_layout(
        xaxis=dict(
            rangeselector=dict(
                buttons=[
                    dict(count=6, label="6m", step="month", stepmode="backward"),
                    dict(count=1, label="YTD", step="year", stepmode="todate"),
                    dict(count=1, label="1y", step="year", stepmode="backward"),
                    dict(step="all"),
                ]
            ),
            type="date",
        ),  # end xaxis  definition
        xaxis1_rangeslider_visible=False,
        xaxis2_rangeslider_visible=False,
        xaxis3_rangeslider_visible=False,
        xaxis4_rangeslider_visible=False,
        xaxis4_type="date",
    )

    return fig


def makefig_squeeze(df, ticker):
    fig = make_subplots(
        rows=4,
        cols=1,
        vertical_spacing=0.012,
        subplot_titles=(
            ticker,
            "TTM Squeeze indicator",
            "Stochastic Oscillator",
            "Kelner channels",
        ),
        shared_xaxes=True,
        row_heights=[0.6, 0.1, 0.1, 0.6],
    )

    fig.add_trace(
        go.Candlestick(
            x=df["Date"],
            open=df["Open"],
            high=df["High"],
            low=df["Low"],
            close=df["Close"],
            name=ticker,
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df.Date, y=df["EMA_8"], line=dict(color="red", width=1), name="EMA8"
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df.Date, y=df["EMA_21"], line=dict(color="orange", width=1), name="EMA21"
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df.Date, y=df["EMA_34"], line=dict(color="yellow", width=1), name="EMA34"
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df.Date, y=df["SMA_50"], line=dict(color="green", width=1), name="SMA50"
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df.Date, y=df["SMA_100"], line=dict(color="blue", width=1), name="SMA100"
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df.Date,
            y=df["SMA_200"],
            line=dict(color="violet", width=2),
            name="SMA200",
        ),
        row=1,
        col=1,
    )

    fig.add_trace(
        go.Scatter(
            x=df[df["Squeeze"] == 1]["Date"],
            y=df[df["Squeeze"] == 1]["Squeeze"].astype(int) - 1,
            mode="markers",
            marker_color="red",
        ),
        row=2,
        col=1,
    )

    customscale = [[0, "coral"], [1, "green"]]

    fig.add_trace(
        go.Bar(
            x=df["Date"],
            y=df["value"],
            marker=dict(color=df["color"], colorscale=customscale),
        ),
        row=2,
        col=1,
    )

    fig.add_trace(
        go.Scatter(
            x=df.Date, y=df["%D"], line=dict(color="orange", width=2), name="%K (Slow)"
        ),
        row=3,
        col=1,
    )
    fig.add_hline(y=60, line_width=2, line_dash="dash", line_color="red", row=3, col=1)
    fig.add_hline(
        y=40, line_width=2, line_dash="dash", line_color="green", row=3, col=1
    )

    fig.add_trace(
        go.Candlestick(
            x=df["Date"],
            open=df["Open"],
            high=df["High"],
            low=df["Low"],
            close=df["Close"],
            name="Kelner channels",
        ),
        row=4,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df.Date, y=df["EMA_21"], line=dict(color="gray", width=1), name="EMA21"
        ),
        row=4,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df.Date,
            y=df["LowerBand1"],
            line=dict(color="violet", width=1),
            name="Lower Band",
        ),
        row=4,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df.Date,
            y=df["UpperBand1"],
            line=dict(color="violet", width=1),
            name="Upper Band",
        ),
        row=4,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df.Date,
            y=df["UpperBand2"],
            line=dict(color="cyan", width=1),
            name="Upper 2 Band",
        ),
        row=4,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df.Date,
            y=df["LowerBand2"],
            line=dict(color="cyan", width=1),
            name="Lower 2 Band",
        ),
        row=4,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df.Date,
            y=df["UpperBand3"],
            line=dict(color="red", width=2),
            name="Upper 3 Band",
        ),
        row=4,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df.Date,
            y=df["LowerBand3"],
            line=dict(color="green", width=2),
            name="Lower 3 Band",
        ),
        row=4,
        col=1,
    )

    fig.update_layout(height=1600)

    fig.update_layout(
        xaxis=dict(
            rangeselector=dict(
                buttons=[
                    dict(count=6, label="6m", step="month", stepmode="backward"),
                    dict(count=1, label="YTD", step="year", stepmode="todate"),
                    dict(count=1, label="1y", step="year", stepmode="backward"),
                    dict(step="all"),
                ]
            ),
            type="date",
        ),  # end xaxis  definition
        xaxis1_rangeslider_visible=False,
        xaxis2_rangeslider_visible=False,
        xaxis3_rangeslider_visible=False,
        xaxis4_rangeslider_visible=False,
        xaxis4_type="date",
    )

    return fig


def maketotfig(dftot):
    fig = make_subplots(
        rows=6,
        cols=1,
        vertical_spacing=0.02,
        subplot_titles=(
            "SPY and Keltner bands ± 2 ATR",
            "SPY vs SPY fair value high/low estimates based on liquidity (FED-TGA-RRP)",
            "VIX",
            "SKEW",
            "Put Call ratio 10SMA ",
            "DXY",
        ),
        shared_xaxes=True,
        row_heights=[0.5, 0.3, 0.2, 0.2, 0.2, 0.2],
    )

    fig.add_trace(
        go.Candlestick(
            x=dftot.index,
            open=dftot["Open"],
            high=dftot["High"],
            low=dftot["Low"],
            close=dftot["Close"],
            name="SPY",
        ),
        row=1,
        col=1,
    )
    # fig.add_trace(go.Scatter(x=ds.Date, y=ds['EMA_21'], line=dict(color='gray', width=1), name="EMA21"), row=1, col=1)
    # fig.add_trace(go.Scatter(x=ds.Date, y=ds['UpperBand1'], line=dict(color='violet', width=1), name="Upper Band"), row=1, col=1)
    # fig.add_trace(go.Scatter(x=ds.Date, y=ds['LowerBand1'], line=dict(color='violet', width=1), name="Lower Band") , row=1, col=1)
    fig.add_trace(
        go.Scatter(
            x=dftot.index,
            y=dftot["UpperBand2"],
            line=dict(color="gray", width=1),
            name="Upper 2 Band",
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=dftot.index,
            y=dftot["LowerBand2"],
            line=dict(color="gray", width=1),
            name="Lower 2 Band",
        ),
        row=1,
        col=1,
    )
    # fig.add_trace(go.Scatter(x=ds.Date, y=ds['UpperBand3'], line=dict(color='red', width=2), name="Upper 3 Band"), row=1, col=1)
    # fig.add_trace(go.Scatter(x=ds.Date, y=ds['LowerBand3'], line=dict(color='green', width=2), name="Lower 3 Band"), row=1, col=1)

    fig.add_trace(
        go.Scatter(
            x=dftot.index,
            y=dftot["SP500"],
            line=dict(color="navy", width=2),
            name="SP500",
        ),
        row=2,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=dftot.index,
            y=dftot["est high"],
            line=dict(color="red", width=1),
            name="est high",
        ),
        row=2,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=dftot.index,
            y=dftot["est low"],
            line=dict(color="green", width=1),
            name="est low",
        ),
        row=2,
        col=1,
    )

    fig.add_trace(
        go.Scatter(
            x=dftot.index, y=dftot["VIX"], line=dict(color="navy", width=2), name="VIX"
        ),
        row=3,
        col=1,
    )
    fig.add_hline(y=20, line_width=2, line_color="red", name="20", row=3, col=1)
    fig.add_hline(y=30, line_width=2, line_color="green", name="30", row=3, col=1)

    fig.add_trace(
        go.Scatter(
            x=dftot.index,
            y=dftot["SKEW"],
            line=dict(color="blue", width=2),
            name="SKEW",
        ),
        row=4,
        col=1,
    )
    fig.add_hline(y=135, line_width=2, line_color="red", name="135", row=4, col=1)
    fig.add_hline(y=115, line_width=2, line_color="green", name="115", row=4, col=1)

    fig.add_trace(
        go.Scatter(
            x=dftot.index,
            y=dftot["PC_SMA10"],
            line=dict(color="goldenrod", width=2),
            name="PC ratio 10SMA",
        ),
        row=5,
        col=1,
    )
    fig.add_hline(y=0.8, line_width=2, line_color="red", name="0.8", row=5, col=1)
    fig.add_hline(y=1, line_width=2, line_color="green", name="1", row=5, col=1)

    fig.add_trace(
        go.Scatter(
            x=dftot.index,
            y=dftot["DXY"],
            line=dict(color="darkgreen", width=2),
            name="DXY",
        ),
        row=6,
        col=1,
    )

    fig.update_layout(xaxis1_rangeslider_visible=False)
    fig.update_layout(height=1200)

    return fig


def make_spy_fig(dm):
    fig = make_subplots(
        rows=2,
        cols=1,
        vertical_spacing=0.016,
        subplot_titles=("SPY", "Kelner channels"),
        shared_xaxes=True,
    )

    fig.add_trace(
        go.Candlestick(
            x=dm.index,
            open=dm["Open"],
            high=dm["High"],
            low=dm["Low"],
            close=dm["Close"],
            name="SPY",
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=dm.index, y=dm["EMA_8"], line=dict(color="red", width=1), name="EMA8"
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=dm.index, y=dm["EMA_21"], line=dict(color="orange", width=1), name="EMA21"
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=dm.index, y=dm["EMA_34"], line=dict(color="yellow", width=1), name="EMA34"
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=dm.index, y=dm["SMA_50"], line=dict(color="green", width=1), name="SMA50"
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=dm.index, y=dm["SMA_100"], line=dict(color="blue", width=1), name="SMA100"
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=dm.index,
            y=dm["SMA_200"],
            line=dict(color="violet", width=2),
            name="SMA200",
        ),
        row=1,
        col=1,
    )

    fig.add_trace(
        go.Candlestick(
            x=dm.index,
            open=dm["Open"],
            high=dm["High"],
            low=dm["Low"],
            close=dm["Close"],
            name="SPY",
        ),
        row=2,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=dm.index,
            y=dm["LowerBand1"],
            line=dict(color="violet", width=1),
            name="Lower Band",
        ),
        row=2,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=dm.index,
            y=dm["UpperBand1"],
            line=dict(color="violet", width=1),
            name="Upper Band",
        ),
        row=2,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=dm.index,
            y=dm["UpperBand2"],
            line=dict(color="cyan", width=1),
            name="Upper 2 Band",
        ),
        row=2,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=dm.index,
            y=dm["LowerBand2"],
            line=dict(color="cyan", width=1),
            name="Lower 2 Band",
        ),
        row=2,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=dm.index,
            y=dm["UpperBand3"],
            line=dict(color="red", width=2),
            name="Upper 3 Band",
        ),
        row=2,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=dm.index,
            y=dm["LowerBand3"],
            line=dict(color="green", width=2),
            name="Lower 3 Band",
        ),
        row=2,
        col=1,
    )

    fig.update_layout(height=1400)
    fig.update_layout(xaxis1_rangeslider_visible=False, xaxis2_rangeslider_visible=True)
    fig.update_xaxes(rangeslider_thickness=0.05)

    return fig


def make_vix_fig(dyvix):
    fig = go.Figure()
    fig.add_trace(
        go.Candlestick(
            x=dyvix.index,
            open=dyvix["Open"],
            high=dyvix["High"],
            low=dyvix["Low"],
            close=dyvix["Close"],
            name="VIX",
        )
    )

    fig.add_trace(
        go.Scatter(
            x=dyvix.index,
            y=dyvix["lowerBB"],
            line=dict(color="gray", width=1),
            name="LowerBB2",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=dyvix.index,
            y=dyvix["upperBB"],
            line=dict(color="gray", width=1),
            name="UpperBB2",
        )
    )

    fig.add_hline(y=20, line_width=2, line_color="darkred", name="20", row=3, col=1)
    fig.add_hline(y=30, line_width=2, line_color="green", name="30", row=3, col=1)
    fig.update_layout(height=600, title="VIX and Bollinger bands")
    return fig


def make_dxy_fig(ddd):
    fig = go.Figure()
    fig.add_trace(
        go.Candlestick(
            x=ddd.index,
            open=ddd["Open"],
            high=ddd["High"],
            low=ddd["Low"],
            close=ddd["Close"],
            name="DXY",
        )
    )
    fig.update_layout(height=600, title="US Dollar Index")
    return fig


def colorsect(name):
    if name == "UPTREND" or name in [
        "BULL phase - Strong Uptrend",
        "BULL phase - Uptrend",
    ]:
        color = "green"
    elif name == "DOWNTREND" or name in [
        "BEAR phase - Strong Downtrend",
        "BEAR phase - Downtrend",
    ]:
        color = "red"
    else:
        color = "black"
    return "color: %s" % color
