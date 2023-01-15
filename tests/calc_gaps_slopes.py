import warnings
import time
import logging

import pandas as pd
import numpy as np
import scipy
from scipy import stats
from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.collection import Collection
from rdflib.namespace import FOAF, RDF, RDFS, SKOS, XSD
from rdflib.serializer import Serializer
from rdfpandas.graph import to_dataframe
from SPARQLWrapper import XML, SPARQLWrapper

warnings.filterwarnings("ignore")

def mod_collector(performance_data,comparison_values):
    gap_size= gap_calc( performance_data, comparison_values)
    trend_slope=trend_calc(performance_data,comparison_values)
    monotonic_pred_df = monotonic_pred(performance_data,comparison_values)
    mod= calc_acheivement(performance_data,comparison_values)
    mod_df=gap_size.merge(trend_slope,on='Measure_Name').merge(monotonic_pred_df,on='Measure_Name').merge(mod,on='Measure_Name')
    mod_df=mod_df.drop_duplicates()

    #mod_df= cal_acheivment_loss(mod_df,comparison_values)
    mod_df["Measure_Name"]=mod_df["Measure_Name"].str.decode(encoding="UTF-8")
    mod_df.to_csv("mod_df.csv",index=False)
    return mod_df

def calc_acheivement(performance_data_df,comparison_values_df):
    performance_data_df['Month'] = pd.to_datetime(performance_data_df['Month'])
    idx= performance_data_df.groupby(['Measure_Name'])['Month'].nlargest(3) .reset_index()
    l=idx['level_1'].tolist()
    latest_measure_df =  performance_data_df[performance_data_df.index.isin(l)]
    latest_measure_df['performance_data'] = latest_measure_df['Passed_Count'] / latest_measure_df['Denominator']
    latest_measure_df['performance_data']=latest_measure_df['performance_data'].fillna(0)
    trend=[]
    performance_data_month1 =[]
    performance_data_month2=[]
    performance_data_month3= []
    peer_average_month1=[]
    peer_average_month2=[]
    peer_average_month3=[]
    trend_df=latest_measure_df.drop_duplicates(subset=['Measure_Name'])
    row1=latest_measure_df.iloc[0]
    Measure_Name =row1['Measure_Name']
    i=0
    for rowIndex, row in latest_measure_df.iterrows():
        if(row['Measure_Name']== Measure_Name and i==0):
            performance_data_month1.append(row['performance_data'])
            peer_average_month1.append(row['Peer_Average'])
            i=i+1
        elif(row['Measure_Name']== Measure_Name and i==1):
            performance_data_month2.append(row['performance_data'])
            peer_average_month2.append(row['Peer_Average'])
            i=i+1
        elif(row['Measure_Name']== Measure_Name and i ==2):
            performance_data_month3.append( row['performance_data'])
            peer_average_month3.append(row['Peer_Average'])
            i=0
        if(row['Measure_Name']!=Measure_Name):
            Measure_Name = row["Measure_Name"]
            performance_data_month1.append(row['performance_data'])
            peer_average_month1.append(row['Peer_Average'])
            i=i+1
    trend_df['performance_data_month1']  = performance_data_month1
    trend_df['performance_data_month2']  = performance_data_month2
    trend_df['performance_data_month3']  = performance_data_month3
    trend_df['peer_average_month1']  = peer_average_month1
    trend_df['peer_average_month2']  = peer_average_month2
    trend_df['peer_average_month3']  = peer_average_month3
    trend_df = trend_df[['Measure_Name','performance_data_month1','performance_data_month2','performance_data_month3','peer_average_month1','peer_average_month2','peer_average_month3']]
    
    # comparison_values_df["slowmo:acceptable_by{URIRef}[0]"].fillna(130, inplace = True)
    # comparison_values_df = comparison_values_df[comparison_values_df['slowmo:acceptable_by{URIRef}[0]']!= 130]
    # comparison_values_df=comparison_values_df.reset_index()
    # comparison_values_df.drop(columns=comparison_values_df.columns[0], axis=1, inplace=True)
    comparison_values_df= comparison_values_df.drop_duplicates()
    #comparison_values_df.Measure_Name = comparison_values_df.Measure_Name.str.encode('utf-8')
   
    trend_df.Measure_Name = trend_df.Measure_Name.str.encode('utf-8')
    trend_df =pd.merge( comparison_values_df,trend_df , on='Measure_Name', how='inner')
    trend_df['comparison_value'] = trend_df['comparison_value'].astype('double') 
    trend_df['performance_data_month1'] = trend_df['performance_data_month1'].astype('double') 
    trend_df['performance_data_month2'] = trend_df['performance_data_month2'].astype('double')
    trend_df['performance_data_month3'] = trend_df['performance_data_month3'].astype('double')
    trend_df['peer_average_month1'] = trend_df['peer_average_month1'].astype('double') 
    trend_df['peer_average_month2'] = trend_df['peer_average_month2'].astype('double')
    trend_df['peer_average_month3'] = trend_df['peer_average_month3'].astype('double')
    trend_df['peer_average_month1']=trend_df['peer_average_month1']/100
    trend_df['peer_average_month2']=trend_df['peer_average_month2']/100
    trend_df['peer_average_month3']=trend_df['peer_average_month3']/100

    trend_df['month1_goal_gap_size']=trend_df['comparison_value']- trend_df['performance_data_month1']
    trend_df['month2_goal_gap_size']=trend_df['comparison_value']- trend_df['performance_data_month2']
    trend_df['month3_goal_gap_size']=trend_df['comparison_value']- trend_df['performance_data_month3']
    trend_df['month1_peer_gap_size']=trend_df['peer_average_month1']- trend_df['performance_data_month1']
    trend_df['month2_peer_gap_size']=trend_df['peer_average_month2']- trend_df['performance_data_month2']
    trend_df['month3_peer_gap_size']=trend_df['peer_average_month3']- trend_df['performance_data_month3']

    loss_predict_goal=[]
    loss_predict_peer=[]
    for rowIndex, row in trend_df.iterrows():
        if(((row['month3_goal_gap_size']>0) and (row['month2_goal_gap_size'])>=0)==True):
            loss_predict_goal.append("no loss/acheivement")
        if(((row['month3_goal_gap_size']<=0) and (row['month2_goal_gap_size'])<=0)==True):
            loss_predict_goal.append("no loss/acheivement")
        if((row['month3_goal_gap_size']<0) and (row['month2_goal_gap_size']>0)==True ):
            loss_predict_goal.append("loss")
        if((row['month3_goal_gap_size']>=0) and (row['month2_goal_gap_size']<0)==True ):
            loss_predict_goal.append("acheivement")

    for rowIndex, row in trend_df.iterrows():
        if(((row['month3_peer_gap_size']>0) and (row['month2_peer_gap_size'])>=0)==True):
            loss_predict_peer.append("no loss/acheivement")
        if(((row['month3_peer_gap_size']<=0) and (row['month2_peer_gap_size'])<=0)==True):
            loss_predict_peer.append("no loss/acheivement")
        if((row['month3_peer_gap_size']<0) and (row['month2_peer_gap_size']>0)==True ):
            loss_predict_peer.append("loss")
        if((row['month3_peer_gap_size']>=0) and (row['month2_peer_gap_size']<0)==True ):
            loss_predict_peer.append("acheivement")
    
    
    # for x in range(len(loss_predict)):
    #     print(loss_predict[x])
    # print(len(loss_predict))
    # print(trend_df.shape)
    time_since_last_acheivement=[]
    trend_df['loss_predict_goal']=loss_predict_goal
    trend_df['loss_predict_peer']=loss_predict_peer
    # for rowIndex, row in trend_df.iterrows():
    #     if((row['loss_predict_goal']=="acheivement")):
    #         r




    trend_df=trend_df[["Measure_Name","loss_predict_goal","loss_predict_peer"]]
    #trend_df.to_csv('trend_df.csv')
    return trend_df






def gap_calc( performance_data_df, comparison_values):
    comparison_values_df = comparison_values
    goal_gap_size_df = calc_goal_comparator_gap(comparison_values_df,performance_data_df)
    goal_gap_size_df['goal_gap_size']=goal_gap_size_df['goal_gap_size'].fillna(0)
    goal_gap_size_df=goal_gap_size_df[["Measure_Name","comparison_type","performance_data","goal_gap_size","peer_gap_size","goal_gap","peer_gap"]]
    goal_gap_size_df=goal_gap_size_df.drop_duplicates()
    #goal_gap_size_df.to_csv('gap_size.csv')
    return goal_gap_size_df

def trend_calc(performance_data_df,comparison_values):
    performance_data_df['Month'] = pd.to_datetime(performance_data_df['Month'])
    lenb= len( comparison_values[['Measure_Name']].drop_duplicates())
    idx= performance_data_df.groupby(['Measure_Name'])['Month'].nlargest(3) .reset_index()
    l=idx['level_1'].tolist()
    latest_measure_df =  performance_data_df[performance_data_df.index.isin(l)]
    latest_measure_df['performance_data'] = latest_measure_df['Passed_Count'] / latest_measure_df['Denominator']
    latest_measure_df['performance_data']=latest_measure_df['performance_data'].fillna(0)
    
    lenb= len( comparison_values[['Measure_Name']])
    # print(lenb)
    out = latest_measure_df.groupby('Measure_Name').apply(theil_reg, xcol='Month', ycol='performance_data')
    
    df_1=out[0]
    df_1 = df_1.reset_index()
    df_1 = df_1.rename({0:"performance_trend_slope"}, axis=1)
    #print(df_1)
    latest_measure_df[['Staff_Number','Measure_Name','Month','Passed_Count','Flagged_Count','Denominator','performance_data']] = latest_measure_df[['Staff_Number','Measure_Name','Month','Passed_Count','Flagged_Count','Denominator','performance_data']].astype(str)
    df_1[[ 'Measure_Name','performance_trend_slope']]= df_1[[ 'Measure_Name','performance_trend_slope']].astype(str)
    #print(df_1)
    comparison_values[['Measure_Name','comparison_type','comparison_value']] = comparison_values[['Measure_Name','comparison_type','comparison_value']].astype(str)
    # comparison_values=comparison_values.Measure_Name.str.encode('utf-8')                               
    # latest_measure_df=latest_measure_df.Measure_Name.str.encode('utf-8')
    # df_1=df_1.Measure_Name.str.encode('utf-8')
    # print(latest_measure_df)
    slope_df = pd.merge( latest_measure_df,df_1 , on='Measure_Name', how='outer')

    slope_df[['Staff_Number','Measure_Name','Month','Passed_Count','Flagged_Count','Denominator','performance_data','performance_trend_slope']]=slope_df[['Staff_Number','Measure_Name','Month','Passed_Count','Flagged_Count','Denominator','performance_data','performance_trend_slope']].astype(str)
    slope_df.Measure_Name=slope_df.Measure_Name.str.encode('utf-8')
    comparison_values.Measure_Name=comparison_values.Measure_Name.str.encode('utf-8')
    # slope_df.to_csv('slope22.csv')
    slope_df=slope_df.drop_duplicates(subset=['Measure_Name'])
    slope_final_df =pd.merge( comparison_values,slope_df , on='Measure_Name', how='outer')
    # slope_final_df.to_csv('slope12.csv')
    slope_final_df = slope_final_df[:(lenb-1)]
    slope_final_df=slope_final_df.drop_duplicates(subset=['Measure_Name'])
    slope_final_df.performance_trend_slope=slope_final_df.performance_trend_slope.astype(float)
    trend_slope=[]
    
    for rowIndex, row in slope_final_df.iterrows():
        if (row['performance_trend_slope']>0):
            trend_slope.append("positive")
        elif(row['performance_trend_slope']==0):
            trend_slope.append("no trend")
        else:
            trend_slope.append("negative")
        
    slope_final_df['trend_slope']= trend_slope   
    slope_final_df['performance_trend_slope'] = slope_final_df['performance_trend_slope'].abs()
    
    slope_final_df=slope_final_df[["Measure_Name",'trend_slope',"performance_trend_slope"]]
    slope_final_df.to_csv('slope.csv')
    return slope_final_df

def theil_reg(df, xcol, ycol):
   model = stats.theilslopes(df[ycol],df[xcol])
   return pd.Series(model)


def calc_goal_comparator_gap(comparison_values_df, performance_data):
    performance_data['Month'] = pd.to_datetime(performance_data['Month'])
    idx= performance_data.groupby(['Measure_Name'])['Month'].transform(max) == performance_data['Month']
    latest_measure_df = performance_data[idx]
    
    performance_data =[]
    for rowIndex, row in latest_measure_df.iterrows():
        if (row['Passed_Count']==0 and row['Denominator']==0):
            performance_data.append(0.0)
        else:
            performance_data.append(row['Passed_Count']/row['Denominator'])
    latest_measure_df['performance_data']=performance_data
    latest_measure_df= latest_measure_df.reset_index(drop=True)
    comparison_values_df= comparison_values_df.reset_index(drop=True)
    # print(latest_measure_df.dtypes)
    # print(comparison_values_df.dtypes)
    latest_measure_df[['Staff_Number','Measure_Name','Month','Passed_Count','Flagged_Count','Denominator','performance_data','Peer_Average']] = latest_measure_df[['Staff_Number','Measure_Name','Month','Passed_Count','Flagged_Count','Denominator','performance_data','Peer_Average']].astype(str)
    comparison_values_df[['Measure_Name','comparison_type','comparison_value']] = comparison_values_df[['Measure_Name','comparison_type','comparison_value']].astype(str)
    latest_measure_df.Measure_Name = latest_measure_df.Measure_Name.str.encode('utf-8')
    comparison_values_df.Measure_Name = comparison_values_df.Measure_Name.str.encode('utf-8')

    # print(latest_measure_df)
    # print(comparison_values_df)
    # print(latest_measure_df.info())
    # print(comparison_values_df.info())
    # comparison_values_df = comparison_values_df.astype({'Measure_Name':'string'})
    # latest_measure_df = latest_measure_df.astype({'Measure_Name':'string'})
    
    # comparison_values_df=comparison_values_df.set_index(['Measure_Name'])
    # latest_measure_df=latest_measure_df.set_index(['Measure_Name'])
    # comparison_values_df.to_csv('comparison.csv',index='False')
    # latest_measure_df.to_csv('latestmeasure.csv',index='False')
    final_df=latest_measure_df.merge(comparison_values_df, how='outer', on=['Measure_Name'])
    #final_df=pd.concat([comparison_values_df,latest_measure_df],axis=1,join='outer')
    #final_df=pd.merge(comparison_values_df,latest_measure_df,left_on='Measure_Name', right_on='Measure_Name',how='outer')
    #final_df=comparison_values_df.join(latest_measure_df,how='outer')
    
    final_df['comparison_value'] = final_df['comparison_value'].astype('double') 
    final_df['Peer_Average'] = final_df['Peer_Average'].astype('double') 
    final_df['Peer_Average'] = final_df['Peer_Average']/100
    final_df=final_df.reset_index(drop=True)
    # final_df.to_csv("finaldf.csv")
    #final_df=final_df.drop_duplicates(subset=['comparison_id'])
    final_df['performance_data']=final_df['performance_data'].astype('double') 
    final_df['goal_gap_size'] = final_df['comparison_value']- final_df['performance_data']
    final_df['peer_gap_size']=final_df['Peer_Average']-final_df['performance_data']
    peer_gap=[]
    goal_gap=[]
    for rowIndex, row in final_df.iterrows():
        if (row['peer_gap_size']>0):
            peer_gap.append("positive")
        elif(row['peer_gap_size']==0):
            peer_gap.append("no gap")
        else:
            peer_gap.append("negative")
        
        if(row['goal_gap_size']>0):
            goal_gap.append("positive")
        elif(row['goal_gap_size']==0):
            goal_gap.append("no gap")
        else:
            goal_gap.append("negative")
   

    final_df['peer_gap']=peer_gap
    final_df['goal_gap']=goal_gap
    final_df['goal_gap_size'] = final_df['goal_gap_size'].abs()
    final_df['peer_gap_size'] = final_df['peer_gap_size'].abs()
    final_df.to_csv('final_df.csv')
    return final_df

def monotonic_pred(performance_data_df,comparison_values_df):
    performance_data_df['Month'] = pd.to_datetime(performance_data_df['Month'])
    idx= performance_data_df.groupby(['Measure_Name'])['Month'].nlargest(3) .reset_index()
    l=idx['level_1'].tolist()
    latest_measure_df =  performance_data_df[performance_data_df.index.isin(l)]
    latest_measure_df['performance_data'] = latest_measure_df['Passed_Count'] / latest_measure_df['Denominator']
    latest_measure_df['performance_data']=latest_measure_df['performance_data'].fillna(0)
    trend=[]
    performance_data_month1 =[]
    performance_data_month2=[]
    performance_data_month3= []
    trend_df=latest_measure_df.drop_duplicates(subset=['Measure_Name'])
    row1=latest_measure_df.iloc[0]
    Measure_Name =row1['Measure_Name']
    i=0
    for rowIndex, row in latest_measure_df.iterrows():
        if(row['Measure_Name']== Measure_Name and i==0):
            performance_data_month1.append(row['performance_data'])
            i=i+1
        elif(row['Measure_Name']== Measure_Name and i==1):
            performance_data_month2.append(row['performance_data'])
            i=i+1
        elif(row['Measure_Name']== Measure_Name and i ==2):
            performance_data_month3.append( row['performance_data'])
            i=0
        if(row['Measure_Name']!=Measure_Name):
            Measure_Name = row["Measure_Name"]
            performance_data_month1.append(row['performance_data'])
            i=i+1
    trend_df['performance_data_month1']  = performance_data_month1
    trend_df['performance_data_month2']  = performance_data_month2
    trend_df['performance_data_month3']  = performance_data_month3
    trend_df = trend_df[['Measure_Name','performance_data_month1','performance_data_month2','performance_data_month3']]
    
    # comparison_values_df["slowmo:acceptable_by{URIRef}[0]"].fillna(130, inplace = True)
    # comparison_values_df = comparison_values_df[comparison_values_df['slowmo:acceptable_by{URIRef}[0]']!= 130]
    # comparison_values_df=comparison_values_df.reset_index()
    # comparison_values_df.drop(columns=comparison_values_df.columns[0], axis=1, inplace=True)
    comparison_values_df= comparison_values_df.drop_duplicates()
    #comparison_values_df.Measure_Name = comparison_values_df.Measure_Name.str.encode('utf-8')
    trend_df.Measure_Name = trend_df.Measure_Name.str.encode('utf-8')
    trend_df =pd.merge( comparison_values_df,trend_df , on='Measure_Name', how='inner')
    for rowIndex, row in trend_df.iterrows():
        m1= row['performance_data_month2']-row['performance_data_month1']
        m2= row['performance_data_month3']-row['performance_data_month2']
        if (m1==0 or m2==0):
            trend.append("no trend")
        elif(m1>0 and m2 <0)or(m1<0 and m2>0):
            trend.append("non-monotonic")
        elif(m1>0 and m2>0) or (m1<0 or m2<0):
            trend.append("monotonic")
    lenc= len(trend)
    trend_df['monotonic_predict'] = trend
    trend_df=trend_df[["Measure_Name","monotonic_predict"]]
    #trend_df.to_csv('trend_df.csv')
    return trend_df