'''
Created on 2019年4月27日

@author: user
'''
import collections
import pandas as pd
import os
import numpy as np
from functools import reduce


class swf:
    def __init__(self,local_sample=['1973-01-01','2025-01-01']):
        wf = collections.OrderedDict()
        self.mysmpl = local_sample
        self.genr_empty_df()
        self.df = collections.OrderedDict()
        
    def importts(self,dir,filetype = 'csv',repeat=True):
        if filetype == 'csv':
            df_raw = pd.read_csv(dir,header=0,index_col=0)
            
            if len(df_raw.index)<0.001:
                _, tail = os.path.split(dir)
                new_name = tail.split('.')[0]
                while new_name in self.df.keys():
                    new_name = new_name + '_new'
                self.df[new_name]=self.empty_df
                
            else:
                df_raw.index = pd.to_datetime(df_raw.index)
                for s in df_raw.columns.tolist():
                    this_df = df_raw[[s]]
                    # should use conversion down to bday here!!!
                    this_df = pd.merge(self.empty_df,this_df,left_index=True,right_index=True,how='left')
                    this_df = this_df[[s]]
                    if repeat:
                        # here should use repeat value between sample
                        last_idx = this_df.last_valid_index()
                        this_df.fillna(method='ffill',inplace=True)
                        this_df.loc[last_idx:,:]=np.nan
                    new_name=s
                    while new_name in self.df.keys():
                        new_name = new_name + '_new'
                    self.df[new_name] = this_df
            
    def genr_empty_df(self):
        date_rng = pd.date_range(start=self.mysmpl[0], end=self.mysmpl[1], freq='B')
        self.empty_df = pd.DataFrame(index=date_rng, columns=[0])
    
    def genr(self,name,const=0):
        df = self.empty_df.copy()
        new_name = name
        while new_name in self.df.keys():
            new_name = new_name + '_new'
        df.columns=[new_name]
        df = const
        self.df[new_name]=df
        return df
    
    def pool_genr(self,pool,poolname,prefix='',suffix='',const=np.nan):
        df = self.empty_df.copy()
        for iso in pool:
            col_name = prefix+iso+suffix
            df[col_name]=const
            df.drop(0,inplace=True)
        while poolname in self.df.keys():
            poolname = poolname+'_new'
        self.df[poolname]=df
        return self.df[poolname]
    
    def pool_makegroup(self,pool,poolname,prefix='',suffix=''):
        for iso in pool:
            key = prefix + iso + suffix
            if not key in self.df.keys():
                print ('Sorry, can not make group if the series is not in the wf: ',key)
                raise ValueError
        dfs = [self.df[prefix+iso+suffix] for iso in pool]
        if len(dfs)==1:
            df_pool=dfs[0]
        else:
            df_pool = reduce(lambda df1,df2: pd.merge(df1,df2,left_index=True,right_index=True,how='outer'), dfs)
        while poolname in self.df.keys():
            poolname = poolname+'_new'
        self.df[poolname]=df_pool
        return self.df[poolname]
    
    def update_df(self,df_name,new_df):
        self.df[df_name]=new_df
    
    def add_df(self,df_name,new_df,repeat=True):
        while df_name in self.df.keys():
            df_name = df_name+'_new'
        new_df = pd.merge(self.empty_df,new_df,left_index=True,right_index=True,how='left')
        new_df.drop(0,axis=1,inplace=True)
        
        if repeat:
            for s in new_df.columns:
                last_idx = new_df.loc[:,[s]].last_valid_index()
                new_df.loc[new_df.index<=last_idx,s].fillna(method='ffill',inplace=True)
            
        self.df[df_name] = new_df
        
    @staticmethod                      
    def divide_df1_df2(df1,df2):
        assert len(df1.columns)==len(df2.columns),'Sorry, the columns of the 2 dfs in devision are not the same'
        
        df1.index = pd.to_datetime(df1.index)
        df2.index = pd.to_datetime(df2.index)
        
        common_dates = df1.index.intersection(df2.index)
        
        df11 = df1.loc[common_dates,:]
        df22 = df2.loc[common_dates,:]
        
        df11 = df11.reset_index(drop=True)
        df22 = df22.reset_index(drop=True)
        
        df11.columns = range(len(df11.columns))
        df22.columns = range(len(df22.columns))
        
        df_final = df11/df22
        df_final.index = common_dates
        df_final.columns = [i+'/('+j+')' for i,j in zip(df1.columns.tolist(),df2.columns.tolist())]
        return df_final
    
    def profit(self,signal_panel,sci_panel,riskcapital,profsmpl):
        '''
        the function uses yesterday's signal and return from yesterday to today to calculate the return
        important: note that the date in return series refer to the day later than the signal
        '''
        assert len(signal_panel.columns)==len(sci_panel.columns),"Sorry, the number of signal col is differnt from number of sci col"
        assert len(signal_panel.index)==len(sci_panel.index),"Sorry, the number of index is different"
        
        ret_panel = sci_panel.pct_change(periods=1)
        ret_panel.columns = range(len(ret_panel.columns))
        
        sig_panel_1 = signal_panel.shift(1)
        sig_panel_1.columns = range(len(sig_panel_1.columns))
        
        sig_panel_1 = sig_panel_1*ret_panel
        sig_panel_1['pnl'] = sig_panel_1.fillna(0).sum(axis=1)
        pnl = sig_panel_1[['pnl']] # dollar pnl
        
        profsmpl = [pd.to_datetime(i) for i in profsmpl]
        
        mask = (pnl.index<=profsmpl[0]) | (pnl.index>profsmpl[1])
        pnl.loc[mask,:]=np.nan
        #pnl.loc[pnl.index==profsmpl[0],:]=riskcapital
        
        pnl['reinvestprof'] = (pnl['pnl']/riskcapital+1).cumprod()*riskcapital
        pnl.loc[pnl.index==profsmpl[0],'reinvestprof']=riskcapital
        pnl['cumprof'] = pnl['pnl'].cumsum()
        return pnl
    
    def returnstats(self,df_equitycurve,profsmpl):
        '''
        return ann_mean, ann_std, ann_sharpe, drawdown_series
        '''
        if not 'reinvestprof' in df_equitycurve.columns:
            print ('Sorry, the reinvestprof is not in the df_equitycurve')
            raise ValueError
        
        mask = (df_equitycurve.index<=profsmpl[0]) | (df_equitycurve.index>=profsmpl[1])
        df1 = df_equitycurve.loc[:,['reinvestprof']]
        df1.loc[mask,:]=np.nan
        
        df1['dpnl'] = df1.iloc[:,0].pct_change(1)*100
        ann_mean = df1['dpnl'].dropna().mean()*252
        ann_std = df1['dpnl'].dropna().std()*np.sqrt(252)
        ann_sharpe = ann_mean/ann_std
        
        df1['drawdown'] = (df1['reinvestprof']-df1['reinvestprof'].cummax())/df1['reinvestprof'].cummax()
        return ann_mean,ann_std,ann_sharpe,df1[['drawdown']]
    
        

class LS_basket:
    def __init__(self):
        pass
    
    @staticmethod
    def LS_filtered_outright(df_indicator,df_sci,conviction=True):
        '''
        trade the instrument based on the signal, outright, the balance is settled by cash position
        If conviction is True, then the position size is related to the score, otherwise, simply long-short
        '''
        assert len(df_indicator.columns)==len(df_sci.columns), "Sorry, indicator group has different column with sci"
        assert len(df_indicator.index)==len(df_sci.index), "Sorry, indicator group has different index with sci"
        df_indicator_ = df_indicator.copy()
        df_sci_ = df_sci.copy()
        
        # step1: filtering out the period where the sci index doesn't exist
        first_idx_list = [df_sci_[[s]].first_valid_index() for s in df_sci_.columns]
        last_idx_list = [df_sci_[[s]].last_valid_index() for s in df_sci_.columns]
        
        counter = 0
        for f,l in zip(first_idx_list,last_idx_list):
            col = df_indicator_.columns.tolist()[counter]
            mask = (df_indicator_.index>l) | (df_indicator_.index<f)
            df_indicator_.loc[mask,col]=np.nan
        
        signal_group = df_indicator.copy()
        signal_group.loc[:,:]=np.nan
        if conviction:
            signal_group = df_indicator_*1000
        else:
            signal_group = ((df_indicator_>0)*2-1)*1000
        signal_group['cash'] = -signal_group.fillna(0).sum(axis=1)
        print ('after cash:',signal_group.dropna())
        return signal_group
    
    @staticmethod
    def HmL_filtered_signal():
        '''
        high minus low portfolio
        '''
        pass

    
    
if __name__ == '__main__':
    new_wf = swf()
    new_wf.importts(r"C:\Users\user\Desktop\s_p500.csv", filetype='csv', repeat=True)
    #print (new_wf.df)
    pool = ['USA','USA']
    df_volume=new_wf.pool_makegroup(pool, 'volume_group',prefix='', suffix='_volume')
    
    #df_indicator.to_csv(r'd:\yangs\13-14-毕业第一年\fx research pasquale\BS0310-1314\codes for building up strategies using eviews\indicator.csv')
    
    df_indicator1 = df_volume.rolling(window=21).mean().shift(2)
    df_indicator2 = df_volume.rolling(window=42).mean().shift(2)
    df = new_wf.divide_df1_df2(df_indicator1, df_indicator2)-1
    new_wf.add_df('indicator_group', df, repeat=True)
    print (df.dropna().head(10))
    
    # create sci_group
    # import sci group
    df_sci = new_wf.pool_makegroup(pool,'sci_group',prefix='',suffix='_SCI')
    print (new_wf.df.keys())
    #print (df_indicator2.dropna())
    new_LS = LS_basket()
    signal_group = new_LS.LS_filtered_outright(new_wf.df['indicator_group'], new_wf.df['sci_group'])
    print (signal_group.dropna().head(10))
    # add cash sci to the sci group, in order to match the 
    new_wf.df['sci_group']['cash']=1
    
    profit_sample = ['2017-01-01','2018-12-31']
    
    new_wf.df['equity_curve'] = new_wf.profit(signal_group, new_wf.df['sci_group'], 1000, profit_sample)
    ann_ret,ann_vol,SR,df_DD = new_wf.returnstats(new_wf.df['equity_curve'], profit_sample)
    
    print(ann_ret,ann_vol,SR)
    df_DD.to_csv(r'd:\yangs\13-14-毕业第一年\fx research pasquale\BS0310-1314\codes for building up strategies using eviews\DD.csv')
    
    #print (new_wf.df['equity_curve'].dropna().head(20))
    new_wf.df['equity_curve'].dropna().to_csv(r'd:\yangs\13-14-毕业第一年\fx research pasquale\BS0310-1314\codes for building up strategies using eviews\pnl.csv')
    #signal_group.dropna().to_csv(r'd:\yangs\13-14-毕业第一年\fx research pasquale\BS0310-1314\codes for building up strategies using eviews\sig.csv')
    