'''
关于变动次数的特征
input:
data:dataframe,原始数据
return：dataframe,user和IpFreq
'''
def IpFreqCalx(data):
    # 转时间格式，升序
    data['Txn_Tm'] = pd.to_datetime(data['Txn_Tm'], errors='coerce')
    data = data.sort_values(['User_Id','Txn_Tm'], ascending=True)
    # 比较每一条记录与前一条的IP
    data['tmp_'] = data['Source_IP'].shift(1)
    data['tmp_'] = (data['Source_IP']!=data['tmp_']).astype(int)
    # 标记每组的第一条记录
    data['tag_'] = data['User_Id'].shift(1)
    data['tag_'] = (data['User_Id']!=data['tag_'])
    data.loc[data['tag_'], 'tmp_'] = 0
    # 计算变动次数
    result = data.groupby(by='User_Id', sort=False)['tmp_'].sum()
    result.name = 'IpFreq'
    result.index.name = 'User_Id'
    return result
  
  
  '''
关于连续次数的特征
input:
data:dataframe,原始数据
list_fail: 失败交易编码列表
return：dataframe,user和ConFailNum
'''
def ConFailNumCalc(data,list_fail):
    # 转小写，字符统一
    list_fail = [xx.lower() for xx in list_fail]
    data['Txn_Tm'] = pd.to_datetime(data['Txn_Tm'], errors='coerce')
    data = data.sort_values(['User_Id', 'Txn_Tm'], ascending=True)
    # 计算每组连续失败次数
    data['tmp1_'] = (data['Txn_Stat_Cd'].isin(list_fail)==False).astype(int)
    data['tmp2_'] =data['tmp1_'].cumsum()
    data = data.set_index('User_Id')
    data_last = data.groupby(level='User_Id', sort=False)['tmp2_'].last()
    data_last = data_last.shift(1).fillna(0)
    data['tmp2_'] = data['tmp2_']-data_last
    data = data.set_index()
    # 计算连续失败最多的次数
    result = data.groupby(by=['User_Id','tmp2_'], sort=False)['tmp2_'].count()-1
    result = result.groupby(level=['User_Id'], sort=False).max()
    result.name = 'ConFailNum'
    result.index.name = 'User_Id'
    return result
