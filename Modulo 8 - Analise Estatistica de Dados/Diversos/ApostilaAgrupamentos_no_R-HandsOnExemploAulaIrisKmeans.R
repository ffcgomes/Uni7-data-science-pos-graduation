km <- function(dados,grupos){
  k <- kmeans(dados,grupos)
  print(table(iris$Species, k$cluster))
  plot(dados, col=k$cluster)
}
km(iris2,3)

library("factoextra")
km2 <- function(dados,grupos){
  k <- kmeans(dados,grupos)
  fviz_cluster(k, iris2, repel = T)
}
km2(iris2,3)