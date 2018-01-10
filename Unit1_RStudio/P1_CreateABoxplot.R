  # Assignment 1, Problem 1
  # Save Homework in my Directory
  mydir <- "C:/Users/house/Google Drive/Grad School/Fall_17_Big Data Analytics/Assignment 1/"
  setwd(mydir)
  
  # Assign n and p variables
  n <- 60.0
  p <- c(0.3,0.5,0.8)
  
  # Create a random distribution using dataframe with n observations
  data <- data.frame(P0.3 = rbinom(n, n, p[1]),
                     P0.5 = rbinom(n, n, p[2]),
                     P0.8 = rbinom(n, n, p[3]))
  
  # Find the Standard deviation of each column
  M <- data.matrix(data, rownames.force = NA)         # First convert df into a matrix
  storage.mode(M) <- "double"                         # convert integers to doubles
  stdev <-c(sd(M[,1]),sd(M[,2]),sd(M[,3]))            # Take standard deviations of cols
  
  # Generate boxplot with data frame
  bp <- boxplot(data,  main="Binomial Distribution, n = 60", 
                xlab="Probability of Successs (p)", ylab="Number of Successes",
                col = c("red","blue","purple"))
  
  # Customize box plot with labels and legend
  text(x = col(bp$stats) - .5, y = bp$stats , labels = bp$stats)
  legend("topleft", inset=.02, title="Standard Deviation",
         legend=c(format(stdev[1], digits=3),
                  format(stdev[2], digits=3),
                  format(stdev[3], digits=3)), 
         pt.bg=c("red", "blue","purple"),
         pch=22)
  
  # Save Work
  # rm(list=ls())
  SaveRdata = paste(mydir,"HW1_D.RData")
  save.image(SaveRdata)