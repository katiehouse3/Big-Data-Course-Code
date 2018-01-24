# Assignment 1, Problem 5
# Create uniform distribution bar chart

m <- matrix(runif(100,min=-1, max=1), nrow=100, ncol=40, byrow = TRUE)
SumRows <- rowSums (m, na.rm = FALSE, dims = 1)
m2 <- cbind(m, newcol)
randcol1 <- m[,15]
randcol2 <- m[,20]

hist1 <- hist(randcol1,
              main=("Uniform Distribution Column 15"),
              breaks=seq(-1,1,by=.5))

