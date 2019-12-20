import pandas as pd
import numpy as np

# create a dummy portfolio
target_vol = 4
np.random.seed(30)
data = np.random.rand(1200,2)
df_dummy = pd.DataFrame(index=pd.bdate_range(periods=1200,start='2000-01-01'),data=data,columns=['strat1_ret','strat2_ret'])
df_dummy.iloc[:100,:]=np.nan
df_dummy.iloc[1100:,:]=np.nan
df_dummy['vol1'] = df_dummy.iloc[:,0].rolling(window=42).std()*np.sqrt(252)*100
df_dummy['vol2'] = df_dummy.iloc[:,1].rolling(window=42).std()*np.sqrt(252)*100
df_dummy['volFactor1'] = target_vol/df_dummy['vol1'].shift(1)
df_dummy['volFactor2'] = target_vol/df_dummy['vol2'].shift(1)
df_dummy['rb1']=1
df_dummy['rb2']=3
df_dummy = df_dummy.loc[:,['strat1_ret','strat2_ret','volFactor1','volFactor2','rb1','rb2']]
#df_dummy.to_csv('dummy_ret.csv')
def risk_budgeting(df_dummy=df_dummy,window=42):
    iloc_ret = [0,1]
    iloc_volFactor = [2,3]
    iloc_rb = [4,5]
    first_Obs = df_dummy.index.get_loc(df_dummy.iloc[:,iloc_volFactor].dropna().index[0])
    last_Obs = df_dummy.index.get_loc(df_dummy.iloc[:, iloc_volFactor].dropna().index[-1])

    iter_last = last_Obs-1 # today's AUM and ret not known yet
    iter_last_less_window = last_Obs-1-window

    # fund vol factor
    df_dummy['fundVolFactor']=np.nan
    iloc_fundVolFactor = len(iloc_ret+iloc_volFactor+iloc_rb)
    df_dummy['scaledRBRet'] = np.nan
    iloc_scaledRBRet = iloc_fundVolFactor+1
    df_dummy['scaledRBProf'] = np.nan
    iloc_scaledRBProf = iloc_scaledRBRet+1
    df_matrix_value = df_dummy.values

    for i in range(first_Obs,last_Obs+1):
        is_start = i-window
        is_end = i
        staticRBRet = np.zeros(window)
        for j in range(len(iloc_ret)):
            this_rb = df_matrix_value[i,iloc_rb[j]]
            this_volFactor = df_matrix_value[i,iloc_volFactor[j]]
            this_ret = df_matrix_value[is_start:is_end,iloc_ret[j]]
            staticRBRet = staticRBRet + this_ret*this_volFactor*this_rb
        this_fund_scale_factor = target_vol/(np.std(staticRBRet)*np.sqrt(252)*100)
        df_matrix_value[i,iloc_fundVolFactor] = this_fund_scale_factor

    # calculate the return after scale
    ret_start = first_Obs+1
    ret_end = last_Obs
    scaledRBRet = np.zeros(ret_end-ret_start)
    print ('scaledRBRet shape is : ',scaledRBRet.shape)
    for k in range(len(iloc_ret)):
        this_ret = df_matrix_value[ret_start:ret_end,iloc_ret[k]]
        #print ('first and last 5 of return : ',this_ret[:5],this_ret[-5:])
        this_volFactor = df_matrix_value[ret_start-1:ret_end-1,iloc_volFactor[k]]
        #print ('first and last 5 of vol factor : ',this_volFactor[:5],this_volFactor[-5:])
        this_rb = df_matrix_value[ret_start - 1:ret_end - 1, iloc_rb[k]]
        #print('first and last 5 of vol factor : ', this_volFactor[:5], this_volFactor[-5:])
        this_fund_scale_factor = df_matrix_value[ret_start-1:ret_end-1,iloc_fundVolFactor]
        #print ('first and last 5 of fund scale factor : ',this_fund_scale_factor[:5],this_fund_scale_factor[-5:])
        scaledRBRet = scaledRBRet + this_ret*this_volFactor*this_rb*this_fund_scale_factor
    df_matrix_value[ret_start:ret_end,iloc_scaledRBRet]=scaledRBRet

    # calculate the cum return

    df_dummy_new = pd.DataFrame(index=df_dummy.index,columns=df_dummy.columns,data=df_matrix_value)


    #iloc_fund_scale_factor = len(df_dummy_new.columns)-1
    # calc the actual return after scaling



    df_dummy_new.to_csv('df_dummy_new.csv')
    return df_dummy_new

def aum():
    # this programme is calc the theoretical AUM for every period by adding returns on yesterday's AUM and the subscrip/redemp at each period
    pass

def actual_ret_after_scaling(df_dummy):
    pass

risk_budgeting()

