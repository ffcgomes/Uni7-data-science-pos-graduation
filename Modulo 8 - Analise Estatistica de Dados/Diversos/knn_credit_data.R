# Leitura da base de dados
base = read.csv('credito.csv')
head(base)
# Apaga a coluna clientid
base$clientid = NULL
head(base)
# Valores inconsistentes
base$age = ifelse(base$age < 0, 40.92, base$age)

# Valores faltantes
base$age = ifelse(is.na(base$age), mean(base$age, na.rm = TRUE), base$age)

# Escalonamento
base[, 1:3] = scale(base[, 1:3])

# Divisão entre treinamento e teste
library(caTools)
set.seed(1)
divisao = sample.split(base$income, SplitRatio = 0.75)
base_treinamento = subset(base, divisao == TRUE)
base_teste = subset(base, divisao == FALSE)

#install.packages('class')
library(class)
previsoes = knn(train = base_treinamento[, -4], test = base_teste[, -4],
                cl = base_treinamento[, 4], k = 5)
matriz_confusao = table(base_teste[,4], previsoes)
print(matriz_confusao)
library(caret)
confusionMatrix(matriz_confusao)

# ZeroR 
table(base_teste$default)

