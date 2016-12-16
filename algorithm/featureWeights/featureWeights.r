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

# PCA特征权重
pcaWeights = function(data,attrIndex){
  dataCov = cov(as.matrix(data[,attrIndex]))
  abs(eigen(dataCov)$vectors[,1])
}

# 相关系数特征权重
corrWeights = function(data,attrIndex,labelIndex){
  weights = numeric(length(attrIndex))
  for (i in 1:length(weights)) weights[i] = abs(cor(as.matrix(data[,c(attrIndex[i],labelIndex)]))[1,2])
  weights
}

# chisquare 特征权重
chisqWeights = function(t){
  chi = chisq.test(t)
  #1 - chi$p.value
  chi$statistic
}

chisqViki = function(t){
  N = sum(t)
  rowP = apply(t,2,sum)/N
  colP = apply(t,1,sum)/N
  stat = 0
  for (i in 1:nrow(t)){
    for (j in 1:ncol(t)){
      numPre = rowP[j]*colP[i]*N
      stat = stat + (t[i,j]-numPre)^2/numPre
    }
  }
  df = (nrow(t)-1)*(ncol(t)-1)
  pchisq(stat,df)
}

##### 特征权重 ####
weightsCompute = function(golf,labelIndex,method){
  weights = c()
  for (i in 1:ncol(golf)){
    if(i != labelIndex) weights = c(weights, method(table(golf[,i],golf[,labelIndex])))
  }
  weights
}


#### 一个示例 y为分类变量 ####
golf = read.csv("golf01.csv",header =F)
colnames(golf) = c("outlook","temperature","humidity","wind","play")

# golf的信息熵
tGolf = table(golf$play)
entropy(tGolf)

# outlook的信息增益、信息增益率、gini系数
informationGain(table(golf$outlook,golf$play))
informationGainRatio(table(golf$outlook,golf$play))
giniIndex(table(golf$outlook,golf$play))

# temperature的pca特征权重
pcaWeights(golf,2)

# golf的信息增益权重和信息增益率权重
weightsCompute(golf,5,informationGain)
weightsCompute(golf,5,informationGainRatio)
weightsCompute(golf,5,giniIndex)
weightsCompute(golf,5,chisqWeights)
weightsCompute(golf,5,chisqViki)
pcaWeights(golf,2:3)
pcaWeights(poly,1:5)

#### 一个示例 y为连续变量 #####
poly = read.csv("Polynominal.csv",header = F)
corrWeights(poly,1:5,6)

