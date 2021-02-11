#Input Data
train<-read.csv('Sobral_Treino.csv')
test<-read.csv('Sobral_Teste.csv')
expl<-read.csv('Sobral_Label.csv')
#combine training data set and its total cases
#train$total_cases<-expl$total_cases
head(train)
head(test)

#See how many percent of NAs in each columns in training and testing data set
colSums(is.na(train))/dim(train)[1]*100


############Delete Identification Columns in Training data set
train$city<-as.factor(train$city)
train$Ano<-NULL
train$Semana<-as.factor(train$Semana)


############Delete Identification Columns in Testing data set
test$city<-as.factor(test$city)
test$Ano<-NULL
test$Semana<-as.factor(test$Semana)

####Create Dummy Variables
library(dummies)
train<-dummy.data.frame(train)
test<-dummy.data.frame(test)

####Random Split as training and validation
random<-sample(1:dim(train)[1])
pct2o3<-floor(dim(train)[1]*2/3)
train_df<-train[random[1:pct2o3],]
vali_df<-train[random[(pct2o3+1):dim(train)[1]],]


lm1<-lm(data = train_df,Infectados~.)
summary(lm1)

#P-value larger than 0.05 would be considered as insignificant. 
#Remove all of that from the model and re-fit it.

#lm2<-lm(data= train_df,Infectados~cityiq+weekofyear36)
#summary(lm2)

#R-squared is really small means that Linear Regression Model may not be the best fit in this case, 
#but still can be a baseline prediction model

#Make prediction on validation dataset and calculate MAE
lmPred<-predict(lm1,vali_df)
MAE_lm<-mean(abs(lmPred-vali_df$Infectados))
cat('MAE_lm:',MAE_lm)

#Support Vector Machine¶

#Build SVM model, try different parameter of C
library(kernlab)
svmOutput1<-ksvm(Infectados~.,data=train_df, kernel="rbfdot",kpar="automatic",C=0.6,cross=3,prob.model=TRUE)
svmOutput2<-ksvm(Infectados~.,data=train_df, kernel="rbfdot",kpar="automatic",C=1,cross=3,prob.model=TRUE)
svmOutput3<-ksvm(Infectados~.,data=train_df, kernel="rbfdot",kpar="automatic",C=1.5,cross=3,prob.model=TRUE)

svmOutput1
svmOutput2
svmOutput3

#As svm1 model has the lowest cross validation error, 
#it is chosed to make further prediction on validation dataset

#Make prediction on validation dataset
svmPred<-predict(svmOutput1,vali_df,type="votes")
MAE_svm<-mean(abs(svmPred-vali_df$Infectados))
MAE_svm

#Random Forest

require(randomForest)
require(MASS)
rf1<-randomForest(train_df$Infectados~.,data=train_df,ntree=50,cross=3)
rf2<-randomForest(train_df$Infectados~.,data=train_df,ntree=250,cross=3)
rf3<-randomForest(train_df$Infectados~.,data=train_df,ntree=500,cross=3)

rf1

#As svm2 model has the lowest cross validation error, 
#it is chosed to make further prediction on validation dataset
#Make prediction on validation dataset
svmPred<-predict(svmOutput2,vali_df,type="votes")
MAE_svm<-mean(abs(svmPred-vali_df$Infectados))
MAE_svm


#Summary
#Linear Regression Model, SVM and Random Forest are tried 
#to make prediction, according to the validation error, SVM2 is chosed.
#There is potential improvement in each model by adjusting different parameters more.
#As for if year should be considered into the model, the best way to say is to try!
