import multiprocessing as mp
import activityFunctions

import pandas_datareader.data as data
import yfinance as yf
import pandas as pd

from datetime import date

from functools import partial

yf.pdr_override()

sp_list = ['AAPL']#['MMM', 'ABT', 'ABBV', 'ABMD', 'ACN', 'ATVI', 'ADBE', 'AMD', 'AAP', 'AES', 'AFL', 'A', 'APD', 'AKAM', 'ALK', 'ALB', 'ARE', 'ALXN', 'ALGN', 'ALLE', 'AGN', 'ADS', 'LNT', 'ALL', 'GOOGL', 'GOOG', 'MO', 'AMZN', 'AMCR', 'AEE', 'AAL', 'AEP', 'AXP', 'AIG', 'AMT', 'AWK', 'AMP', 'ABC', 'AME', 'AMGN', 'APH', 'ADI', 'ANSS', 'ANTM', 'AON', 'AOS', 'APA', 'AIV', 'AAPL', 'AMAT', 'APTV', 'ADM', 'ARNC', 'ANET', 'AJG', 'AIZ', 'ATO', 'T', 'ADSK', 'ADP', 'AZO', 'AVB', 'AVY', 'BKR', 'BLL', 'BAC', 'BK', 'BAX', 'BDX', 'BRK.B', 'BBY', 'BIIB', 'BLK', 'BA', 'BKNG', 'BWA', 'BXP', 'BSX', 'BMY', 'AVGO', 'BR', 'BF.B', 'CHRW', 'COG', 'CDNS', 'CPB', 'COF', 'CPRI', 'CAH', 'KMX', 'CCL', 'CAT', 'CBOE', 'CBRE', 'CDW', 'CE', 'CNC', 'CNP', 'CTL', 'CERN', 'CF', 'SCHW', 'CHTR', 'CVX', 'CMG', 'CB', 'CHD', 'CI', 'XEC', 'CINF', 'CTAS', 'CSCO', 'C', 'CFG', 'CTXS', 'CLX', 'CME', 'CMS', 'KO', 'CTSH', 'CL', 'CMCSA', 'CMA', 'CAG', 'CXO', 'COP', 'ED', 'STZ', 'COO', 'CPRT', 'GLW', 'CTVA', 'COST', 'COTY', 'CCI', 'CSX', 'CMI', 'CVS', 'DHI', 'DHR', 'DRI', 'DVA', 'DE', 'DAL', 'XRAY', 'DVN', 'FANG', 'DLR', 'DFS', 'DISCA', 'DISCK', 'DISH', 'DG', 'DLTR', 'D', 'DOV', 'DOW', 'DTE', 'DUK', 'DRE', 'DD', 'DXC', 'ETFC', 'EMN', 'ETN', 'EBAY', 'ECL', 'EIX', 'EW', 'EA', 'EMR', 'ETR', 'EOG', 'EFX', 'EQIX', 'EQR', 'ESS', 'EL', 'EVRG', 'ES', 'RE', 'EXC', 'EXPE', 'EXPD', 'EXR', 'XOM', 'FFIV', 'FB', 'FAST', 'FRT', 'FDX', 'FIS', 'FITB', 'FE', 'FRC', 'FISV', 'FLT', 'FLIR', 'FLS', 'FMC', 'F', 'FTNT', 'FTV', 'FBHS', 'FOXA', 'FOX', 'BEN', 'FCX', 'GPS', 'GRMN', 'IT', 'GD', 'GE', 'GIS', 'GM', 'GPC', 'GILD', 'GL', 'GPN', 'GS', 'GWW', 'HRB', 'HAL', 'HBI', 'HOG', 'HIG', 'HAS', 'HCA', 'PEAK', 'HP', 'HSIC', 'HSY', 'HES', 'HPE', 'HLT', 'HFC', 'HOLX', 'HD', 'HON', 'HRL', 'HST', 'HPQ', 'HUM', 'HBAN', 'HII', 'IEX', 'IDXX', 'INFO', 'ITW', 'ILMN', 'IR', 'INTC', 'ICE', 'IBM', 'INCY', 'IP', 'IPG', 'IFF', 'INTU', 'ISRG', 'IVZ', 'IPGP', 'IQV', 'IRM', 'JKHY', 'J', 'JBHT', 'SJM', 'JNJ', 'JCI', 'JPM', 'JNPR', 'KSU', 'K', 'KEY', 'KEYS', 'KMB', 'KIM', 'KMI', 'KLAC', 'KSS', 'KHC', 'KR', 'LB', 'LHX', 'LH', 'LRCX', 'LW', 'LVS', 'LEG', 'LDOS', 'LEN', 'LLY', 'LNC', 'LIN', 'LYV', 'LKQ', 'LMT', 'L', 'LOW', 'LYB', 'MTB', 'M', 'MRO', 'MPC', 'MKTX', 'MAR', 'MMC', 'MLM', 'MAS', 'MA', 'MKC', 'MXIM', 'MCD', 'MCK', 'MDT', 'MRK', 'MET', 'MTD', 'MGM', 'MCHP', 'MU', 'MSFT', 'MAA', 'MHK', 'TAP', 'MDLZ', 'MNST', 'MCO', 'MS', 'MOS', 'MSI', 'MSCI', 'MYL', 'NDAQ', 'NOV', 'NTAP', 'NFLX', 'NWL', 'NEM', 'NWSA', 'NWS', 'NEE', 'NLSN', 'NKE', 'NI', 'NBL', 'JWN', 'NSC', 'NTRS', 'NOC', 'NLOK', 'NCLH', 'NRG', 'NUE', 'NVDA', 'NVR', 'ORLY', 'OXY', 'ODFL', 'OMC', 'OKE', 'ORCL', 'PCAR', 'PKG', 'PH', 'PAYX', 'PAYC', 'PYPL', 'PNR', 'PBCT', 'PEP', 'PKI', 'PRGO', 'PFE', 'PM', 'PSX', 'PNW', 'PXD', 'PNC', 'PPG', 'PPL', 'PFG', 'PG', 'PGR', 'PLD', 'PRU', 'PEG', 'PSA', 'PHM', 'PVH', 'QRVO', 'PWR', 'QCOM', 'DGX', 'RL', 'RJF', 'RTN', 'O', 'REG', 'REGN', 'RF', 'RSG', 'RMD', 'RHI', 'ROK', 'ROL', 'ROP', 'ROST', 'RCL', 'SPGI', 'CRM', 'SBAC', 'SLB', 'STX', 'SEE', 'SRE', 'NOW', 'SHW', 'SPG', 'SWKS', 'SLG', 'SNA', 'SO', 'LUV', 'SWK', 'SBUX', 'STT', 'STE', 'SYK', 'SIVB', 'SYF', 'SNPS', 'SYY', 'TMUS', 'TROW', 'TTWO', 'TPR', 'TGT', 'TEL', 'FTI', 'TFX', 'TXN', 'TXT', 'TMO', 'TIF', 'TJX', 'TSCO', 'TDG', 'TRV', 'TFC', 'TWTR', 'TSN', 'UDR', 'ULTA', 'USB', 'UAA', 'UA', 'UNP', 'UAL', 'UNH', 'UPS', 'URI', 'UTX', 'UHS', 'UNM', 'VFC', 'VLO', 'VAR', 'VTR', 'VRSN', 'VRSK', 'VZ', 'VRTX', 'VIAC', 'V', 'VNO', 'VMC', 'WRB', 'WAB', 'WMT', 'WBA', 'DIS', 'WM', 'WAT', 'WEC', 'WFC', 'WELL', 'WDC', 'WU', 'WRK', 'WY', 'WHR', 'WMB', 'WLTW', 'WYNN', 'XEL', 'XRX', 'XLNX', 'XYL', 'YUM', 'ZBRA', 'ZBH', 'ZION', 'ZTS']

calls_or_puts = 'puts'
expiry = '2020-08-20'


pool = mp.Pool(mp.cpu_count())

if calls_or_puts == 'calls':
    call = partial(activityFunctions.unusualActivity_calls, exp_date=expiry)
    final = pool.map(call, sp_list)

else:
    call = partial(activityFunctions.unusualActivity_puts, exp_date=expiry)
    final = pool.map(call, sp_list)


pool.close()    
pool.join()

returned = pd.concat(final)

returned = returned.drop(columns = ['contractSymbol', 'lastTradeDate', 'contractSize', 'currency'])
returned.insert(3, 'Distance OTM', returned['stock_price'] - returned['strike'])
returned['Value'] = returned['openInterest']*returned['lastPrice']*100
returned['Pct_OTM'] = returned['Distance OTM']/returned['stock_price']
if calls_or_puts == 'calls':
    returned=returned[returned['Pct_OTM']<-.1]
else:
    returned=returned[returned['Pct_OTM']>.1]
    
returned.to_csv('unusual_'+calls_or_puts+'_activity_'+expiry[6:]+'_'+str(date.today())+'.csv')

returned.sort_values(by=['Value'])
returned = returned[['Symbol', 'stock_price', 'strike', 'lastPrice','percentChange', 'volume', 'openInterest', 'impliedVolatility', 'V/OI', 'Value', 'Pct_OTM']].copy()
returned = returned.rename(columns={'stock_price':'Stock Price', 'strike':'Strike', 'lastPrice': 'Last Price', 'percentChange':'% Change', 'volume':'Volume', 'openInterest':'Open Interest', 'impliedVolatility':'IV', 'Pct_OTM': '% OTM'})
returned['% OTM'] = returned['% OTM']*100
returned = returned.iloc[:10,:]
f = open('unusual_activity_'+str(date.today())[6:]+'_'+str(date.today())+'.txt', 'w')
f.write('## Top Puts Traded '+str(date.today())[6:]+' by Value (Volume * Price)\n')
f.write(returned.to_markdown(tablefmt="pipe", index=False))
f.write('\n')


calls_or_puts = 'calls'
expiry = '2020-08-20'

pool = mp.Pool(mp.cpu_count())

if calls_or_puts == 'calls':
    call = partial(activityFunctions.unusualActivity_calls, exp_date=expiry)
    final = pool.map(call, sp_list)

else:
    call = partial(activityFunctions.unusualActivity_puts, exp_date=expiry)
    final = pool.map(call, sp_list)

print('done')

pool.close()    
pool.join()

returned = pd.concat(final)

returned = returned.drop(columns = ['contractSymbol', 'lastTradeDate', 'contractSize', 'currency'])
returned.insert(3, 'Distance OTM', returned['stock_price'] - returned['strike'])
returned['Value'] = returned['openInterest']*returned['lastPrice']*100
returned['Pct_OTM'] = returned['Distance OTM']/returned['stock_price']
if calls_or_puts == 'calls':
    returned=returned[returned['Pct_OTM']<-.1]
else:
    returned=returned[returned['Pct_OTM']>.1]
    
returned.to_csv('unusual_'+calls_or_puts+'_activity_'+expiry[6:]+'_'+str(date.today())+'.csv')

returned.sort_values(by=['Value'])
returned = returned[['Symbol', 'stock_price', 'strike', 'lastPrice','percentChange', 'volume', 'openInterest', 'impliedVolatility', 'V/OI', 'Value', 'Pct_OTM']].copy()
returned = returned.rename(columns={'stock_price':'Stock Price', 'strike':'Strike', 'lastPrice': 'Last Price', 'percentChange':'% Change', 'volume':'Volume', 'openInterest':'Open Interest', 'impliedVolatility':'IV', 'Pct_OTM': '% OTM'})
returned['% OTM'] = returned['% OTM']*100
returned = returned.iloc[:10,:]
f.write('## Top Calls Traded '+str(date.today())[6:]+' by Value (Volume * Price)\n')
f.write(returned.to_markdown(tablefmt="pipe", index=False))
f.write('\n')
f.close()
