

MVP goal: 
Recruitment for software tech industry is changing and currently around 74% of Hiring manager and Recruiter focus on evidence for short listing a candidate. Additional training cost per hire is estimated to be 19k. To reduce the cost involved in hiring and to shorten the process I propose to shortlist candidates based on the GitHub activity. In this project I design a tool AceCoder for recruiters to  parse and search through 3TB of logged data to find and rank a set of candidates. Search can be local or non-local.

** GitHub is huge data set of activity logs of software project and code repositories associated with a user.

Engineering challenges: 
1. Finding candidates in certain location, by computing the distance from the  location of job. -Performance of search.
2. Rate the candidates all over the US to match the requirements of position. Focus on matching the job requirements to all the individual users in the database.
3. Candidates evaluation criterion:
  - Number of followers, forks for the repo.
  - Number of repo activity in the specific language e.g. commit, push over time if possible.

Data Set: 
https://bigquery.cloud.google.com/results/mapdemo-221219:US.bquijob_29602955_16d375892a6
