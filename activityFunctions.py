from datetime import date
import pandas_datareader.data as data
import yfinance as yf
import pandas as pd

yf.pdr_override()

def unusualActivity_calls(stock, exp_date):
    x = stock
    try:
        ticker = yf.Ticker(x)
        opt = ticker.option_chain(exp_date).calls.copy()
        opt.insert(0, 'Exp_Date', exp_date)
        opt.insert(0, 'Symbol', x)
        opt.insert(3, 'stock_price', data.get_data_yahoo(x, end_date = date.today())['Close'][-1])
        opt['V/OI'] =opt['volume'].astype('float')/opt['openInterest']
        return opt[opt['volume'] > 200]
    except:
        pass

def unusualActivity_puts(stock, exp_date):
    x = stock
    try:
        ticker = yf.Ticker(x)
        opt = ticker.option_chain(exp_date).puts.copy()
        opt.insert(0, 'Exp_Date', exp_date)
        opt.insert(0, 'Symbol', x)
        opt.insert(3, 'stock_price', data.get_data_yahoo(x, end_date = date.today())['Close'][-1])
        opt['V/OI'] =opt['volume'].astype('float')/opt['openInterest']
        return opt[opt['volume'] > 200]
    except:
        pass
