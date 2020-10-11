# Data processing for Taximizer

Data processing was performed in Spark running on a distributed cluster. The cluster used consisted of 1 m5.large controller and 4 c5.large workers. With this cluster setup processing takes ~11.5 minutes. 

Data aggregation occurs in three main steps: 
1. The cab data is aggregated from 1 record per ride to 1 record per hour per region
1. The hourly cab data is joined to hourly weather data
1. The joined hourly data is aggregated again from 1 record per hour to 1 record per [day, hour, area, temperature, precipitation] combination

![image](DataFlow.png)