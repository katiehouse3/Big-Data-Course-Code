# Assignment 1, Problem 4
# Create Box Plot of old faithful

library("ggplot2")
library(reshape)

# import Data
duration = faithful$eruptions; # the eruption durations
waiting = faithful$waiting; # the waiting period

# Create a dataframe with eruption data
faithful <- data.frame(duration,
                   waiting)

# Create an if statement for new column in dataframe
for (i in 1 : nrow(faithful)){
  if (faithful$duration[i] < 3.1) {
    faithful$type[i] <-"short"
  } else {
    faithful$type[i] <-"long"
  }
}

meltdata <- melt(faithful, id = "type")
meltdata$typevariable <- interaction(meltdata$type, meltdata$variable)

# Generate boxplot with data frame
bp <- ggplot(aes(y = value, x = typevariable, fill = type), data = meltdata) + geom_boxplot() 
bp + ylim(0, 80)
bp + labs(x = "Eruption Type", y = "data", title="Old Faithful Eruption Data")
