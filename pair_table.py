from pathlib import Path  # Standard Python Module
import pandas as pd
from cmath import nan
from datetime import datetime
import os
import pandas as pd
import numpy as np
from source_dir import SOURCE_DIR
#from headingsPanel import *


'AIRdata001.csv'

class Pair_table():
    
    def __init__ (self, stockname, source, file):
        self.pairTable(stockname, source, file)
        
    
    def pairTable(self, stockname, source, file):
        
        df_ = pd.read_csv(f'{SOURCE_DIR}/{source}/{file}')
        
        df_['Date'] = pd.to_datetime(df_['Date']) #formatting
        df_['Volume'] = df_['Volume'].replace(',','').astype(int)
        df_['Open'] = df_['Open'].astype(float)
        df_['High'] = df_['High'].astype(float)
        df_['Low'] = df_['Low'].astype(float)
        df_['Close'] = df_['Close'].astype(float)
        df_ = df_.sort_values(by="Date")
        
        #Volume
        df_['Vol40Ave'] = df_['Volume'].rolling(window=40, min_periods=1).mean() #needs work
        df_['VolAgstAve'] = df_['Volume']/df_['Vol40Ave']
        df_['VolAgstAve-9'] = df_['VolAgstAve'].shift(9)
        df_['VolAgstAve-8'] = df_['VolAgstAve'].shift(8)
        df_['VolAgstAve-7'] = df_['VolAgstAve'].shift(7)
        df_['VolAgstAve-6'] = df_['VolAgstAve'].shift(6)
        df_['VolAgstAve-5'] = df_['VolAgstAve'].shift(5)
        df_['VolAgstAve-4'] = df_['VolAgstAve'].shift(4)
        df_['VolAgstAve-3'] = df_['VolAgstAve'].shift(3)
        df_['VolAgstAve-2'] = df_['VolAgstAve'].shift(2)
        df_['VolAgstAve-1'] = df_['VolAgstAve'].shift(1)
        df_['Date'] = pd.to_datetime(df_['Date'])
        
        #Donchien
        df_['10maxHigh'] = round(df_['High'].rolling(window=10).max(), 2)
        df_['10minlow'] = round(df_['Low'].rolling(window=10).min(), 2)
        df_['highminave'] = (df_['10maxHigh']+df_['10minlow'])/2
        df_['highminaveVSclose'] = round(((df_['highminave']/df_['Close'])-1), 4)
        df_['donhighclose'] = round(((df_['Close']-df_['10maxHigh'])/df_['Close']), 4)
        df_['donlowclose'] = round(((df_['Close']-df_['10minlow'])/df_['Close']), 4)
        
        #Res
        df_['1res'] = (df_['Close'].shift(-1)-(df_['Close']))/df_['Close']
        df_['2res'] = (df_['Close'].shift(-2)-(df_['Close']))/df_['Close']
        df_['3res'] = (df_['Close'].shift(-3)-(df_['Close']))/df_['Close']
        df_['4res'] = (df_['Close'].shift(-4)-(df_['Close']))/df_['Close']
        df_['5res'] = (df_['Close'].shift(-5)-(df_['Close']))/df_['Close']
        df_['6res'] = (df_['Close'].shift(-6)-(df_['Close']))/df_['Close']
        df_['7res'] = (df_['Close'].shift(-7)-(df_['Close']))/df_['Close']
        df_['8res'] = (df_['Close'].shift(-8)-(df_['Close']))/df_['Close']
        df_['9res'] = (df_['Close'].shift(-9)-(df_['Close']))/df_['Close']
        df_['10res'] = (df_['Close'].shift(-10)-(df_['Close']))/df_['Close']
        df_['11res'] = (df_['Close'].shift(-11)-(df_['Close']))/df_['Close']
        df_['12res'] = (df_['Close'].shift(-12)-(df_['Close']))/df_['Close']
        df_['13res'] = (df_['Close'].shift(-13)-(df_['Close']))/df_['Close']
        df_['14res'] = (df_['Close'].shift(-14)-(df_['Close']))/df_['Close']
        df_['15res'] = (df_['Close'].shift(-15)-(df_['Close']))/df_['Close']
        
        #bkchg
        df_['1bkchg'] = (df_['Close']-(df_['Close'].shift(1)))/df_['Close'].shift(1)
        df_['2bkchg'] = (df_['Close']-(df_['Close'].shift(2)))/df_['Close'].shift(2)
        df_['3bkchg'] = (df_['Close']-(df_['Close'].shift(3)))/df_['Close'].shift(3)
        df_['4bkchg'] = (df_['Close']-(df_['Close'].shift(4)))/df_['Close'].shift(4)
        df_['5bkchg'] = (df_['Close']-(df_['Close'].shift(5)))/df_['Close'].shift(5)
        df_['6bkchg'] = (df_['Close']-(df_['Close'].shift(6)))/df_['Close'].shift(6)
        df_['7bkchg'] = (df_['Close']-(df_['Close'].shift(7)))/df_['Close'].shift(7)
        df_['8bkchg'] = (df_['Close']-(df_['Close'].shift(8)))/df_['Close'].shift(8)
        df_['9bkchg'] = (df_['Close']-(df_['Close'].shift(9)))/df_['Close'].shift(9)
        df_['10bkchg'] = (df_['Close']-(df_['Close'].shift(10)))/df_['Close'].shift(10)
        
        df_['5bk-10bk'] = (df_['10bkchg'])-(df_['5bkchg'])
        
        df_['closeToHighChg'] = (df_['High']-df_['Close'].shift(1))/(df_['Close'].shift(1))
        df_['closeToHighChg-1'] = df_['closeToHighChg'].shift(1)
        df_['closeToHighChg-2'] = df_['closeToHighChg'].shift(2)
        df_['closeToHighChg-3'] = df_['closeToHighChg'].shift(3)
        df_['closeToHighChg-4'] = df_['closeToHighChg'].shift(4)
        
        df_['closeToLowChg'] = (df_['Close'].shift(1)-df_['Low'])/(df_['Close'].shift(1))
        df_['closeToLowChg-1'] = df_['closeToLowChg'].shift(1)
        df_['closeToLowChg-2'] = df_['closeToLowChg'].shift(2)
        df_['closeToLowChg-3'] = df_['closeToLowChg'].shift(3)
        df_['closeToLowChg-4'] = df_['closeToLowChg'].shift(4)        
        
        #boll
        df_['20dayAve'] = df_['Close'].rolling(window=20).mean()
        df_['20dayStdev'] = df_['Close'].rolling(20).std()
        df_['bollUpper'] = df_['20dayAve']+df_['20dayStdev']*2
        df_['bollUpperArea'] = (df_['bollUpper']-df_['Close'])/df_['Close']
        df_['bollUpperAreaChg'] = df_['bollUpperArea'] - df_['bollUpperArea'].shift(1)
        df_['bollLower'] = df_['20dayAve']-df_['20dayStdev']*2
        df_['bollLowerArea'] = (df_['bollLower']-df_['Close'])/df_['Close']
        df_['bollLowerAreaChg'] = df_['bollLowerArea'] - df_['bollLowerArea'].shift(1)
       
        #RSI
        df_['rsiBaseChg'] = (df_['Close']-df_['Close'].shift(1))
        df_.loc[df_['rsiBaseChg'] <= 0, 'rsiUpwardMovement'] = 0 
        df_.loc[df_['rsiBaseChg'] > 0, 'rsiUpwardMovement'] = (df_['rsiBaseChg'])
        df_.loc[df_['rsiBaseChg'] <= 0, 'rsiDownwardMovement'] = (df_['rsiBaseChg'])
        df_.loc[df_['rsiBaseChg'] > 0, 'rsiDownwardMovement'] = 0
        
        df_['3weekUpperMedian'] = round((df_['rsiUpwardMovement'].rolling(15).median()), 4)
        df_['3weekLowerMedian'] = round((df_['rsiDownwardMovement'].rolling(15).median()), 4)
        df_['AverageUpwardMovement'] = df_['rsiUpwardMovement'].rolling(14).mean()
        df_['WeightedAverageUpwardMovement'] = ((df_['AverageUpwardMovement'].shift(1)*13)+df_['rsiUpwardMovement'])/14
        df_['AverageDownwardMovement'] = df_['rsiDownwardMovement'].rolling(14).mean()
        df_['WeightedAverageDownwardMovement'] = ((df_['AverageDownwardMovement'].shift(1)*13)+df_['rsiDownwardMovement'])/14
        df_['RelativeStrength'] = df_['WeightedAverageUpwardMovement']/df_['WeightedAverageDownwardMovement']
        
        df_['RSI'] = 100-100/(df_['RelativeStrength']+1)
        df_['rsiCHG'] = df_['RSI']-df_['RSI'].shift(1)
        df_['rsiCHG-1'] = df_['rsiCHG'].shift(1)
        df_['rsiCHG-2'] = df_['rsiCHG'].shift(2)
        df_['rsiCHG-5'] = df_['rsiCHG'].shift(5)
        df_['rsiCHG-10'] = df_['rsiCHG'].shift(10)
        df_['rsiCHG-15'] = df_['rsiCHG'].shift(15)
        
        #rollingAverages
        df_['50dayAverage'] = df_['Close'].rolling(50).mean()
        df_['50aveComp'] = (df_['Close'] - df_['50dayAverage'])/df_['Close']
        df_['200dayAverage'] = df_['Close'].rolling(200).mean()
        df_['200dayComp'] = (df_['Close'] - df_['200dayAverage'])/df_['Close']
        df_['AveCross'] = (df_['50dayAverage']-df_['200dayAverage'])/df_['200dayAverage']
        df_['AveCrossChg'] = df_['AveCross'] - df_['AveCross'].shift(1)
        df_ = df_.copy()
        #Scholastic
        df_['Stochastic14minVSnow'] = df_['Close']-(df_['Low'].rolling(14).min())
        df_['Stochastic14DayminVSmax'] = (df_['High'].rolling(14).max())-(df_['Low'].rolling(14).min())
        df_['StochasticK'] = df_['Stochastic14minVSnow']/df_['Stochastic14DayminVSmax']
        df_['Stochastic%K'] = df_['StochasticK'].rolling(3).mean()
        df_['Stochastic%D'] = df_['Stochastic%K'].rolling(3).mean()
        df_['StochKDdiff'] = df_['Stochastic%K'] - df_['Stochastic%D']
        
        #Support and Resistance 
        df_['Pivot point (PP)'] = (df_['High']+df_['Low']+df_['Close'])/3
        df_['First resistance (R1)'] = (2*df_['Pivot point (PP)'])-df_['Low']
        df_['R1Chg'] = (df_['Close']-df_['First resistance (R1)'])/df_['Close']
        df_['Second resistance (R2)'] = df_['Pivot point (PP)']+(df_['High']-df_['Low'])
        df_['R2Chg'] = (df_['Close']-df_['Second resistance (R2)'])/df_['Close']
        df_['Third resistance (R3)'] = df_['High']+(2*(df_['Pivot point (PP)']-df_['Low']))
        df_['R3Chg'] = (df_['Close']-df_['Third resistance (R3)'])/df_['Close']
        
        df_['First support (S1)'] = (2*df_['Pivot point (PP)'])-df_['High']
        df_['S1Chg'] = (df_['Close']-df_['First support (S1)'])/df_['Close']
        df_['Second support (S2)'] = df_['Pivot point (PP)']-(df_['High']-df_['Low'])
        df_['S2Chg'] = (df_['Close']-df_['Second support (S2)'])/df_['Close']
        df_['Third support (S3)'] = df_['Low']-(2*(df_['High']-df_['Pivot point (PP)']))
        df_['S3Chg'] = (df_['Close']-df_['Third support (S3)'])/df_['Close']
        
        df_['RS1diff'] = round((df_['R1Chg']-df_['S1Chg']), 4)
        df_['RS2diff'] = round((df_['R2Chg']-df_['S2Chg']), 4)
        df_['RS3diff'] = round((df_['R3Chg']-df_['S3Chg']), 4)
        
        
        df_.to_csv(os.path.join(f'Documents/StocksPython/Data',file))
        df_ = pd.read_csv(f'{SOURCE_DIR}/{source}/{file}')
        df_ = df_.sort_values(by="Date", axis=0, ascending=False)
        df_.to_csv(os.path.join(f'Documents/StocksPython/Data',file))
        
        
