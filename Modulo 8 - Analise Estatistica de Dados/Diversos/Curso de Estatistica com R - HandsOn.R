" Regressão Linear"
x <- c(201,225,305,380,560,600,685,735)
y <- c(17,20,21,23,25,24,27,27)
dados = data.frame(x,y)  # Cria DataFrame
is.data.frame(dados)

regressao = lm(y~x, data=dados)
regressao
summary(regressao)

"Assim como a maioria das funções do R, armazenamos os resultados retornados pela
função lm() em um objeto. O valor retornado por lm() é uma lista. Siga os comandos:
"
is.list(regressao)
names(regressao)
z= plot(x,y)
grid(z)
abline(regressao)

plot(regressao)
cor(x,y)


