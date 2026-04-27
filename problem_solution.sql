-- DATA 351 Final - Problem Solution
-- Ethan Bucy, Teo Mendoza, Ben Webster

WITH state_party_totals AS (
    -- Sum up all votes for each party in each state
    SELECT 
        stateid 
        , name
        , party 
        , SUM(candidatevotes) AS total_state_votes
    FROM electionresults
    WHERE year = 2024
    GROUP BY 
        stateid
        , name
        , party
),
ranked_popular_vote AS (
    -- Rank the parties by their total vote count
    SELECT 
        stateid 
        , name
        , party 
        , total_state_votes
        , ROW_NUMBER() OVER (
            PARTITION BY stateid 
            ORDER BY total_state_votes DESC
        ) as rnk
    FROM state_party_totals 
),
state_politics AS (
    -- Select the winning party of the state popular vote
    SELECT 
        stateid 
        , name
        , party 
        , total_state_votes
    FROM ranked_popular_vote 
    WHERE rnk = 1
),
state_sightings AS (
    -- Calculate the number of UFO sightings per state
    SELECT 
        states.statename AS state
        , states.stateid 
        , COUNT(ufosightings.id) AS num_sightings
    FROM states
    JOIN ufosightings ON states.stateid = ufosightings.stateid
    GROUP BY 
        states.statename
        , states.stateid
),
state_bases AS (
    -- Calculate the number of military bases per state
    SELECT 
        states.statename AS state
        , states.stateid 
        , COUNT(bases.id) AS num_bases
    FROM states
    JOIN bases ON states.stateid = bases.stateid
    GROUP BY 
        states.statename
        , states.stateid
)
-- Combine everything into the final query!
SELECT 
    ss.state
    , round(ss.num_sightings/sb.num_bases, 3) AS ufo_sightings_per_base
    , ss.num_sightings
    , sb.num_bases
    , sp.party AS political_alignment
FROM state_sightings AS ss
JOIN state_bases AS sb ON ss.stateid = sb.stateid
JOIN state_politics AS sp ON ss.stateid = sp.stateid
ORDER BY ufo_sightings_per_base DESC;