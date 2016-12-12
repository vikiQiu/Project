######### data #########
train.raw=read.csv("../data/train.csv")
test.raw=read.csv("../data/test.csv")
df.train=train.raw
df.test=test.raw

######### visualization ########
# names(df.train) # label, pixel N
# balance of the data
df.train$label2=as.factor(df.train$label)
barplot(table(df.train$label2),main="Number-Frequency",col="firebrick")
# missing data: No Missing Data
sum(is.na(df.train))
sum(is.na(df.test))
# learn the total density of the pixel
x=NULL
for (i in 2:785) x=c(x,df.train[,i])
hist(x,col = "firebrick") # most are 0 and 255

######### PCA ###########
# variable transformation
x.train=df.train[,2:785]/255
y.train=df.train[,786]
Xcov = cov(x.train)
pcaX = prcomp(Xcov)
vcum = as.data.frame(pcaX$sdev^2/sum(pcaX$sdev^2))
vcum = cbind(1:784,vcum,cumsum(vcum))
colnames(vcum)=c("pixel","variance","cumulative variance")
plot(vcum$'cumulative variance'~vcum$pixel,type='b',
     xlab="Pixel Variables", ylab = "Cumulative Variance",
     main="Fig (2.3) Scree Plot for PCA")
max(vcum$pixel[vcum$`cumulative variance`<=0.99])
x.final=as.matrix(x.train) %*% pcaX$rotation[,1:43]
x.final1=x.final[-ind,];x.final2=x.final[ind,]
y.train1=y.train[-ind];y.train2=y.train[ind]

######### NN ##########
library(nnet)
t1=Sys.time()
nn=nnet(x.final,class.ind(y.train),size=150,softmax=TRUE,maxit=130,MaxNWts = 80000)
t2=Sys.time();t2-t1 # 9.5min
pre.nn=predict(nn,x.final2,type="class")
t.nn=table(pre.nn,y.train2)
1-sum(diag(t.nn))/sum(t.nn)
# 参数选择
set.seed(100)
ind=sample(1:42000,5000)
size.try=seq(5,50,5)
seed.try=seq(99,999,200)
error.try=matrix(0,length(size.try),length(seed.try))
for (i in 1:length(size.try)) {
  for (j in 1:length(seed.try)){
    set.seed(seed.try[j])
    t1=Sys.time()
    nn.try=nnet(x.final1,class.ind(y.train1),size=size.try[i],maxit=130,softmax = T)
    t2=Sys.time();t2-t1 # 21s
    pre.try=predict(nn.try,x.final2,type = "class")
    t.try=table(pre.try,y.train2)
    error.try[i,j]=1-sum(diag(t.try))/sum(t.try)
  }
}

######### NB ##########
library(klaR)
nb.try=NaiveBayes(x.final1,y.train1,fL=2)
pre.nb=predict(nb.try,x.final2,type="class")
t.nb=table(pre.nb$class,y.train2)
1-sum(diag(t.nb))/sum(t.nb);rm(t.nb)
