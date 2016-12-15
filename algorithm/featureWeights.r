#### 基本算子 #####
# 信息熵
entropy = function(x){
  p = x/sum(x)
  entropy = 0
  for (i in p) {if(i!=0) entropy = entropy - i*log2(i)}
  entropy
}

# 信息增益
informationGain = function(t){
  gain = entropy(apply(t,2,sum))
  rowsum = apply(t,1,sum)
  for (i in 1:nrow(t)) gain = gain - rowsum[i]*entropy(t[i,])/sum(t)
  gain
}

# 计算信息增益率中的IV
IVCompute = function(t){
  rowSum = apply(t,1,sum)
  rowP = rowSum/sum(rowSum)
  IV=0
  for(i in 1:length(rowSum)) {if(rowP[i]!=0) IV = IV-rowP[i]*log2(rowP[i])}
  IV
}

# 计算信息增益率
informationGainRatio = function(t){
  informationGain(t)/IVCompute(t)
}

# 计算gini系数
gini = function(t){
  gini = 0
  for(i in t){
    prob = i/sum(t)
    gini = gini + prob*(1-prob)
  }
  gini
}

giniIndex = function(t){
  giniIndex = 0
  rowSums = apply(t,1,sum)
  giniTotal = gini(apply(t,2,sum))
  for (i in 1:nrow(t)){
    giniIndex = giniIndex + rowSums[i]*gini(t[i,])/sum(rowSums)
  }
  giniTotal - giniIndex
}

##### 特征权重 ####
weightsCompute = function(golf,labelIndex,method){
  weights = c()
  for (i in 1:ncol(golf)){
    if(i != labelIndex) weights = c(weights, method(table(golf[,i],golf[,labelIndex])))
  }
  weights
}


#### 一个示例 ####
golf = read.csv("golf01.csv",header =F)
colnames(golf) = c("outlook","temperature","humidity","wind","play")

# golf的信息熵
tGolf = table(golf$play)
entropy(tGolf)

# outlook的信息增益、信息增益率、gini系数
informationGain(table(golf$outlook,golf$play))
informationGainRatio(table(golf$outlook,golf$play))
giniIndex(table(golf$outlook,golf$play))

# golf的信息增益权重和信息增益率权重
weightsCompute(golf,5,informationGain)
weightsCompute(golf,5,informationGainRatio)
weightsCompute(golf,5,giniIndex)


