# Assignment 1, Problem 2
# Create scatter plot of waiting vs. duration times

#Import Data
duration = faithful$eruptions; # the eruption
waiting = faithful$waiting; # the waiting interval
head(cbind(duration, waiting)); #import & bind the old faithful data

#Make scatterplot
plot(duration, waiting, xlab="Eruption duration",
     ylab="Time waited", main="Old Faithful Eruption")

formula = waiting ~ duration
data = faithful

model = lm(formula, data)
abline(model, col='red', lwd = 2)

#Print the y-intercept and slope
print(model)
