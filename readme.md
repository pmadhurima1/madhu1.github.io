

MVP goal: 
Recruitement department of my company is looking for suitable candidates to hire in near future. One part of the hiring decision is based on GitHub activity. 
Given a certain location, find a set suitable candidates that have a certain software proficiency.

Engineering challenges: 
1. Finding candidates in certain location, by computing the distance from the  location of job. -Performance of search.
2. Rate the candidates to match the requirements of the job profile- Performance of ML algorithm such as KNN which is used to match the job requirements to condidate profile.
3. Rate the candidates all over the US to match the requirements of position. Focus on matching the job requirements to all the individual users in the database.
4. Candidates evaluation criterion:
  - Number of followers, forks for the repo.
  - Number of repo activity in the specific language e.g. commit, push over time if possible.

Data Set: 
https://bigquery.cloud.google.com/results/mapdemo-221219:US.bquijob_29602955_16d375892a6
