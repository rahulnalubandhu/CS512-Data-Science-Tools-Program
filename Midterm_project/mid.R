
```{r setup, include=FALSE}
knitr::opts_chunk$set(echo = TRUE)

library(ggplot2)
library(magrittr)
library(dplyr)
library(tidyverse)
library(data.table)
```

# Load libraries
library(dplyr)
library(ggplot2)

# Load the data into R
data <- read.csv("~/Downloads/business_details (1).csv", head = T)



# Calculate the correlation coefficient
correlation <- cor(data$ReviewCount, data$AverageStars)
cat(correlation)




# Clean and manipulate the data

# AverageStars= It is the result of taking the average of the stars field for each business_id

ggplot(data, aes(x = AverageStars, y = ReviewCount )) +
  geom_point(color = "red", alpha = 0.7,size = 3, shape = "circle") +
  labs(x = "Average of stars given for each business", y = "Total number of reviews") +
  ggtitle("Relationship between the number of reviews and average rating of a restaurant")