# Project Title : ACE coder

Recruitment tool to find GitHub users.
[Presentation](https://docs.google.com/presentation/d/1rHsal4DjAMQrGeXlf2AFXeYpI9uFLzka9hd9vZe-AI0/edit?usp=sharing)

<hr/>

## How to install and get it up and running


<hr/>

## Introduction
Recruitment for software tech industry is changing and currently around 74% of Hiring manager and Recruiter focus on evidence for short listing a candidate. Additional training cost per hire is estimated to be 19k. To reduce the cost involved in hiring and to shorten the process I propose to shortlist candidates based on the GitHub activity. In this project I design a tool AceCoder for recruiters to  parse and search through 3TB of logged data to find and rank a set of candidates. Search can be local or non-local

** GitHub is huge data set of activity logs of software project and code repositories associated with a user.

1. Finding candidates in certain location, by computing the distance from the  location of job.
2. Rate the candidates all over the US to match the requirements of position. Focus on matching the job requirements to all the individual users in the database.
3. Candidates evaluation criterion:
  - Number of followers, forks for the repo.
  - Number of repo activity in the specific language e.g. commit, push.

## Architecture
![alt text](https://github.com/madhu1/madhu1.github.io/blob/master/css/Architecture.png)

## Dataset
GitHub Torrent is a huge data set monitoring the public event activity of 36 Million GitHub users [link](http://ghtorrent.org/). GitHub Torrent project uses above data set to extract  1.5 billion rows of extracted metadata. This data is in the form of MySQL dump and the details are available here [link] (http://ghtorrent.org/relational.html) 
https://bigquery.cloud.google.com/results/mapdemo-221219:US.bquijob_29602955_16d375892a6

## Engineering challenges

Bottle Neck: Performance of Joins for processing the data. 

## Trade-offs
