
## DATA-351 Final Problem Statement

### Overview
In this problem, you will write a query that uses the **states**, **ufosightings**, **bases**, and **electionresults** tables loaded in using the provided **clean_up.sql** file. 

### Tables 
* **states**: State names and ids
* **ufosightings**: Individual UFO sightings
* **bases**: Domestic US military bases
* **electionresults**: County-level presidential election data. Each entry represents a single candidate's votes from a single county. 

### Learning Objectives
* **Ch 6**: Join three or more tables in a single query
* **Ch 11**: Write PARTITION BY and ORDER BY in OVER clauses
* **Ch 12**: Use CTEs (WITH) for multi-step queries

### Instructions
Get the ratio of UFO Sightings to Military Bases **ufo_sightings_per_base** for each state. Include the actual number of UFO Sightings **num_sightings** and Military Bases **num_bases** in the query. Additionally, include the political party that recieved the most amount of total votes in 2024 for each state as **political_alignment**. Sort by **ufo_sightings_per_base** descending.  

### Hints
* It is important to break this problem down into pieces- CTEs are your friend!
* **ufo_sightings_per_base** can be computed as (**num_sightings**/**num_bases**).
* Don't forget to filter for election results pertaining exclusively to 2024.