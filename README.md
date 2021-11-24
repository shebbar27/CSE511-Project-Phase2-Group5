# CSE511-Project-Phase2-Group5

Requirement

In this project, you are required to do spatial hot spot analysis. In particular, you need to complete two different hot spot analysis tasks.
1. Hot zone analysis

This task will need to perform a range join operation on a rectangle datasets and a point dataset. For each rectangle, the number of points located within the rectangle will be obtained. The hotter rectangle means that it includes more points. So this task is to calculate the hotness of all the rectangles.

2. Hot cell analysis

Description

This task will focus on applying spatial statistics to spatio-temporal big data in order to identify statistically significant spatial hot spots using Apache Spark. The topic of this task is from ACM SIGSPATIAL GISCUP 2016.

The Problem Definition page is here: http://sigspatial2016.sigspatial.org/giscup2016/problem

The Submit Format page is here: http://sigspatial2016.sigspatial.org/giscup2016/submit

Special requirement (different from GIS CUP)

As stated in the Problem Definition page, in this task, you are asked to implement a Spark program to calculate the Getis-Ord statistic of NYC Taxi Trip datasets. We call it "Hot cell analysis"

To reduce the computation power need, we made the following changes:

    The input will be a monthly taxi trip dataset from 2009 - 2012. For example, "yellow_tripdata_2009-01_point.csv", "yellow_tripdata_2010-02_point.csv".

    Each cell unit size is 0.01 * 0.01 in terms of latitude and longitude degrees.

    You should use 1 day as the Time Step size. The first day of a month is step 1. Every month has 31 days.

    You only need to consider Pick-up Location.

    We don't use Jaccard similarity to check your answer. However, you don't need to worry about how to decide the cell coordinates because the code template generated cell coordinates. You just need to write the rest of the task.
