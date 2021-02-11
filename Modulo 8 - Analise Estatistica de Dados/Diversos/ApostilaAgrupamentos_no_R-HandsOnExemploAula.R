# Cria um vetor com os dados das X,Y e Z
vetor = c(0,0,0,1,0,1,2,1,2,1,0,1,0,0,1)
vetor
#Transforma em uma matriz
base = matrix(vetor,3,5)
#Cria os nomes das linhas
rownames(base) = c("X","Y","Z")
base

#Calcula a distancia entre os casos
d = dist(base, method = "euclidean")
d

# Considerando a distancia euclidiana ao quadrado
d2 = d^2
d2

"Obtenção de Clusters pelo método hierárquico com vários métodos de
ligação: simples, completa, média e Ward."

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

#Verifique e analise completa
groups
