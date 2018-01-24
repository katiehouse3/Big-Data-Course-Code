# Assignment 1, Problem 3
# Calculate the covariance matrix of the faithful data

duration = faithful$eruptions; # the eruption durations
waiting = faithful$waiting; # the waiting period

M = cbind(duration, waiting) # form a matrix with the two observations
cvM = cov(M) #calculate a covariance matrix
ev = eigen(cvM)

#determine eigenvalue
e.value = ev$values
#determine eigenvector
e.vector = ev$vectors

#the eigenvector with the larger eigenvalue
max.e.vector=e.vector[,1]

#calculate points from max vector
p1 = c(0,0)
p2 = c(p1[1]+max.e.vector[1],p1[2]+max.e.vector[2])
m = (p2[2]-p1[2]) / (p2[1]-p1[1]) #the slope of the eigenvector

print(m)