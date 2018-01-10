# Assignment 1, Problem 6
# Create Gaussian curve plot

# Create a 100 row and 40 column matrix
m <- matrix(runif(100,min=-1, max=1), nrow=100, ncol=40, byrow = TRUE)

# Sum the 40 columns in new column
Sumcols <- colSums (m, na.rm = FALSE, dims = 1)

# Bind the matrix with the sum column
m2 <- rbind(m, Sumcols)

# Create a histogram with the sum column
hist <- hist(Sumcols,main=("Column Sum Histogram"),breaks=5)

# Get Sumcol parameters
sumcol.mean = mean(Sumcols)
sumcol.sd = sd(Sumcols)

# Create a cumulative normal distribution
sumcol.x <- seq(
  qnorm(0.001, sumcol.mean, sumcol.sd), 
  qnorm(0.999, sumcol.mean, sumcol.sd), 
  length.out = 40
)

# Add the normal distribution curve
lines(
  sumcol.x , 
  40 * dnorm(sumcol.x, sumcol.mean, sumcol.sd) * 15, 
  col = "red"
)
