"ALGORITMO DAS K-MÉDIAS

PASSO 1 Especifique o número  k  de grupos a serem criados.

PASSO 2 Selecione arbitrariamente k   pontos como centros dos grupos (centróides).

PASSO 3 Atribua cada observação ao grupo de centróide mais próximo, baseado na distância euclidiana entre a observação e os centróides.

PASSO 4 Recalcule os centróides com os pontos atribuídos a cada grupo. O centróide do  j-ésimo grupo é um vetor de comprimento  p   contendo as médias das  p   variáveis,calculadas com todos os pontos atribuídos ao  j -ésimo grupo.

PASSO 5 Repita os passos 3 e 4 até que as atribuições não mais reduzam a soma de quadrados intra-grupo, ou que o número máximo de iterações (ou qualquer outro critério de parada) seja atingido."

# Exemplo retirado da apostila estatisticaclassica.com


iris # Banco de dados das flores
iris2 = scale(iris[-5])  #Escalonamento dos dados
iris2
S=rowSums(iris2)
S
k=2

zab <- k*(S - min(S))/(1.01*max(S)-min(S)) + 1 # atribuição de Zabala (2019) baseada em Hartigan (1975)
(g <- floor(zab)) # grupos
