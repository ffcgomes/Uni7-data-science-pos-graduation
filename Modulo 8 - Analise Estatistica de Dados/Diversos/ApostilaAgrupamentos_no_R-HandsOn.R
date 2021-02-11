
vetor = c(1,2,3,4,5,6,50,35,32,52,30,45,70,45,45,60,40,48)
class(vetor)
vetor
base = matrix(vetor,6,3)
colnames(base) = c("Empresa","Gastos","Vendas")
base

#Calcula a distancia entre os casos
d = dist(base, method = "euclidean")
d

# Considerando a distancia euclidiana ao quadrado
d2 = d^2
d2

grupos.s = hclust(d2,method="single")  #Ligação simples
grupos.c = hclust(d2,method="complete")  #Ligação Completa
grupos.m = hclust(d2,method="average")  #Ligação Média
grupos.w = hclust(d2,method="ward.D")  #Método de Ward

# Dendrograma
# Selecione um dos métodos anteriores

clstr = grupos.s
plot(clstr)

# Selecione um numero de grupos adequado para ser observado
groups = cutree(clstr, k=3) #Cut = 3
rect.hclust(grupos.w, k=3, border = "red")
clstr[]

#Verifieuq e analise completa
groups
