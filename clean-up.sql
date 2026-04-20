--------------------------
--- Database Clear
--------------------------

DROP TABLE IF EXISTS UfoSightingsRaw CASCADE;
DROP TABLE IF EXISTS UfoSightings CASCADE;
DROP TABLE IF EXISTS ElectionResultsRaw CASCADE;
DROP TABLE IF EXISTS ElectionResults CASCADE;
DROP TABLE IF EXISTS BasesRaw CASCADE;
DROP TABLE IF EXISTS Bases CASCADE;
DROP TABLE IF EXISTS States CASCADE;


--------------------------
--- Shared States Table
--------------------------

CREATE TABLE States (
    StateId bigserial PRIMARY KEY,
    StateCode CHAR(2) NOT NULL UNIQUE,
    StateName TEXT NOT NULL UNIQUE
);

CREATE INDEX idx_states_statecode ON States(StateCode);

INSERT INTO States (StateCode, StateName)
VALUES
    ('AL', 'Alabama'),
    ('AK', 'Alaska'),
    ('AZ', 'Arizona'),
    ('AR', 'Arkansas'),
    ('CA', 'California'),
    ('CO', 'Colorado'),
    ('CT', 'Connecticut'),
    ('DE', 'Delaware'),
    ('FL', 'Florida'),
    ('GA', 'Georgia'),
    ('HI', 'Hawaii'),
    ('ID', 'Idaho'),
    ('IL', 'Illinois'),
    ('IN', 'Indiana'),
    ('IA', 'Iowa'),
    ('KS', 'Kansas'),
    ('KY', 'Kentucky'),
    ('LA', 'Louisiana'),
    ('ME', 'Maine'),
    ('MD', 'Maryland'),
    ('MA', 'Massachusetts'),
    ('MI', 'Michigan'),
    ('MN', 'Minnesota'),
    ('MS', 'Mississippi'),
    ('MO', 'Missouri'),
    ('MT', 'Montana'),
    ('NE', 'Nebraska'),
    ('NV', 'Nevada'),
    ('NH', 'New Hampshire'),
    ('NJ', 'New Jersey'),
    ('NM', 'New Mexico'),
    ('NY', 'New York'),
    ('NC', 'North Carolina'),
    ('ND', 'North Dakota'),
    ('OH', 'Ohio'),
    ('OK', 'Oklahoma'),
    ('OR', 'Oregon'),
    ('PA', 'Pennsylvania'),
    ('RI', 'Rhode Island'),
    ('SC', 'South Carolina'),
    ('SD', 'South Dakota'),
    ('TN', 'Tennessee'),
    ('TX', 'Texas'),
    ('UT', 'Utah'),
    ('VT', 'Vermont'),
    ('VA', 'Virginia'),
    ('WA', 'Washington'),
    ('WV', 'West Virginia'),
    ('WI', 'Wisconsin'),
    ('WY', 'Wyoming');


--------------------------
--- UFO Sightings
--------------------------

CREATE TABLE UfoSightingsRaw (
    Id bigserial PRIMARY KEY,
    Sighting TEXT,
    Occurred TEXT,
    Location TEXT,
    Shape TEXT,
    Duration TEXT,
    "No of observers" TEXT,
    Reported TEXT,
    Posted TEXT,
    Summary TEXT,
    Text TEXT,
    "Lights on object" TEXT,
    "Aura or haze around object" TEXT,
    "Aircraft nearby" TEXT,
    "Animals reacted" TEXT,
    "Left a trail" TEXT,
    "Emitted other objects" TEXT,
    "Changed Colo" TEXT,
    "Emitted beams" TEXT,
    "Location details" TEXT,
    "Changed Color" TEXT,
    "Electrical or magnetic effects" TEXT,
    Explanation TEXT,
    "Possible abduction" TEXT,
    "Missing Time" TEXT,
    "Marks found on body afterwards" TEXT,
    Landed TEXT
);

COPY UfoSightingsRaw (
    Sighting,
    Occurred,
    Location,
    Shape,
    Duration,
    "No of observers",
    Reported,
    Posted,
    Summary,
    Text,
    "Lights on object",
    "Aura or haze around object",
    "Aircraft nearby",
    "Animals reacted",
    "Left a trail",
    "Emitted other objects",
    "Changed Colo",
    "Emitted beams",
    "Location details",
    "Changed Color",
    "Electrical or magnetic effects",
    Explanation,
    "Possible abduction",
    "Missing Time",
    "Marks found on body afterwards",
    Landed
)
FROM '/private/tmp/ufos.csv'
WITH (
    FORMAT csv,
    HEADER true
);

CREATE TABLE UfoSightings (
    Id bigserial PRIMARY KEY,
    SightingId bigint NOT NULL UNIQUE,
    OccurredAtDate DATE NOT NULL,
    OccurredAtTime TIME NOT NULL,
    City TEXT NOT NULL,
    StateId bigint NOT NULL,
    Shape TEXT NOT NULL,
    ObserverCount INTEGER NOT NULL,
    Summary TEXT NOT NULL,
    FOREIGN KEY (StateId) REFERENCES States(StateId)
);

CREATE INDEX idx_ufo_stateid ON UfoSightings(StateId);
CREATE INDEX idx_ufo_date ON UfoSightings(OccurredAtDate);
CREATE INDEX idx_ufo_shape ON UfoSightings(Shape);
CREATE INDEX idx_ufo_city ON UfoSightings(City);

INSERT INTO UfoSightings (
    SightingId,
    OccurredAtDate,
    OccurredAtTime,
    City,
    StateId,
    Shape,
    ObserverCount,
    Summary
)
WITH Cleaned AS (
    SELECT
        TRIM(Sighting) AS SightingValue,
        CAST(LEFT(TRIM(Occurred), 19) AS timestamp) AS OccurredLocalTimestamp,
        TRIM(SUBSTRING(TRIM(Location) FROM '^(.*),\s*[^,]+,\s*USA$')) AS CityValue,
        UPPER(TRIM(SUBSTRING(TRIM(Location) FROM '.*,\s*([^,]+),\s*USA$'))) AS StateCodeValue,
        TRIM(Shape) AS ShapeValue,
        CAST(CAST(TRIM("No of observers") AS numeric) AS integer) AS ObserverCountValue,
        TRIM(Summary) AS SummaryValue
    FROM UfoSightingsRaw
    WHERE TRIM(Sighting) <> ''
      AND TRIM(Sighting) ~ '^[0-9]+$'
      AND TRIM(Occurred) <> ''
      AND LEFT(TRIM(Occurred), 19) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$'
      AND TRIM(Location) ~ ',\s*[^,]+,\s*USA$'
      AND TRIM(Shape) <> ''
      AND TRIM(Summary) <> ''
      AND TRIM("No of observers") <> ''
      AND TRIM("No of observers") ~ '^[0-9]+(\.[0-9]+)?$'
)
SELECT
    CAST(C.SightingValue AS bigint),

    CAST(
        CASE
            WHEN C.StateCodeValue IN ('CA', 'WA', 'OR', 'NV', 'AZ') THEN C.OccurredLocalTimestamp
            WHEN C.StateCodeValue IN ('ID', 'UT', 'MT', 'WY', 'CO', 'NM') THEN C.OccurredLocalTimestamp - interval '1 hour'
            WHEN C.StateCodeValue IN ('ND', 'SD', 'NE', 'KS', 'OK', 'TX', 'MN', 'IA', 'MO', 'AR', 'LA', 'WI', 'IL', 'MS', 'AL') THEN C.OccurredLocalTimestamp - interval '2 hours'
            WHEN C.StateCodeValue IN ('MI', 'IN', 'KY', 'TN', 'GA', 'FL', 'SC', 'NC', 'VA', 'WV', 'OH', 'PA', 'NY', 'VT', 'NH', 'ME', 'MA', 'RI', 'CT', 'NJ', 'DE', 'MD') THEN C.OccurredLocalTimestamp - interval '3 hours'
            WHEN C.StateCodeValue = 'AK' THEN C.OccurredLocalTimestamp + interval '1 hour'
            WHEN C.StateCodeValue = 'HI' THEN C.OccurredLocalTimestamp + interval '2 hours'
            ELSE C.OccurredLocalTimestamp
        END AS DATE
    ),

    CAST(
        CASE
            WHEN C.StateCodeValue IN ('CA', 'WA', 'OR', 'NV', 'AZ') THEN C.OccurredLocalTimestamp
            WHEN C.StateCodeValue IN ('ID', 'UT', 'MT', 'WY', 'CO', 'NM') THEN C.OccurredLocalTimestamp - interval '1 hour'
            WHEN C.StateCodeValue IN ('ND', 'SD', 'NE', 'KS', 'OK', 'TX', 'MN', 'IA', 'MO', 'AR', 'LA', 'WI', 'IL', 'MS', 'AL') THEN C.OccurredLocalTimestamp - interval '2 hours'
            WHEN C.StateCodeValue IN ('MI', 'IN', 'KY', 'TN', 'GA', 'FL', 'SC', 'NC', 'VA', 'WV', 'OH', 'PA', 'NY', 'VT', 'NH', 'ME', 'MA', 'RI', 'CT', 'NJ', 'DE', 'MD') THEN C.OccurredLocalTimestamp - interval '3 hours'
            WHEN C.StateCodeValue = 'AK' THEN C.OccurredLocalTimestamp + interval '1 hour'
            WHEN C.StateCodeValue = 'HI' THEN C.OccurredLocalTimestamp + interval '2 hours'
            ELSE C.OccurredLocalTimestamp
        END AS TIME
    ),

    C.CityValue,
    S.StateId,
    C.ShapeValue,
    C.ObserverCountValue,
    C.SummaryValue
FROM Cleaned C
JOIN States S
    ON S.StateCode = C.StateCodeValue
WHERE C.CityValue <> ''
  AND C.ObserverCountValue > 0;


--------------------------
--- Politics / Elections
--------------------------

CREATE TABLE ElectionResultsRaw (
    Id bigserial PRIMARY KEY,
    State TEXT,
    CountyName TEXT,
    Year TEXT,
    StatePo TEXT,
    CountyFips TEXT,
    Office TEXT,
    Candidate TEXT,
    Party TEXT,
    CandidateVotes TEXT,
    TotalVotes TEXT,
    Version TEXT,
    Mode TEXT
);

COPY ElectionResultsRaw (
    State,
    CountyName,
    Year,
    StatePo,
    CountyFips,
    Office,
    Candidate,
    Party,
    CandidateVotes,
    TotalVotes,
    Version,
    Mode
)
FROM '/private/tmp/politics.csv'
WITH (
    FORMAT csv,
    HEADER true
);

CREATE TABLE ElectionResults (
    Id bigserial PRIMARY KEY,
    Name TEXT NOT NULL,
    CountyName TEXT NOT NULL,
    Year INTEGER NOT NULL,
    StateId bigint NOT NULL,
    Office TEXT NOT NULL,
    Candidate TEXT NOT NULL,
    Party TEXT NOT NULL,
    CandidateVotes INTEGER NOT NULL,
    TotalVotes INTEGER NOT NULL,
    FOREIGN KEY (StateId) REFERENCES States(StateId)
);

CREATE INDEX idx_election_stateid ON ElectionResults(StateId);
CREATE INDEX idx_election_year ON ElectionResults(Year);
CREATE INDEX idx_election_county ON ElectionResults(CountyName);
CREATE INDEX idx_election_candidate ON ElectionResults(Candidate);

INSERT INTO ElectionResults (
    Name,
    CountyName,
    Year,
    StateId,
    Office,
    Candidate,
    Party,
    CandidateVotes,
    TotalVotes
)
SELECT
    S.StateName,
    INITCAP(TRIM(R.CountyName)),
    CAST(TRIM(R.Year) AS INTEGER),
    S.StateId,
    CASE
        WHEN UPPER(TRIM(R.Office)) = 'US PRESIDENT' THEN 'US President'
        ELSE INITCAP(LOWER(TRIM(R.Office)))
    END,
    INITCAP(LOWER(TRIM(R.Candidate))),
    INITCAP(LOWER(TRIM(R.Party))),
    CAST(TRIM(R.CandidateVotes) AS INTEGER),
    CAST(TRIM(R.TotalVotes) AS INTEGER)
FROM ElectionResultsRaw R
JOIN States S
    ON S.StateCode = UPPER(TRIM(R.StatePo))
WHERE TRIM(R.CountyName) <> ''
  AND TRIM(R.Year) <> ''
  AND TRIM(R.Year) <> 'NA'
  AND TRIM(R.StatePo) <> ''
  AND TRIM(R.Office) <> ''
  AND TRIM(R.Candidate) <> ''
  AND TRIM(R.Party) <> ''
  AND TRIM(R.CandidateVotes) <> ''
  AND TRIM(R.CandidateVotes) <> 'NA'
  AND TRIM(R.TotalVotes) <> ''
  AND TRIM(R.TotalVotes) <> 'NA'
  AND UPPER(TRIM(R.Candidate)) NOT IN ('OTHER', 'UNDERVOTES', 'OVERVOTES', 'UNDERVOTE', 'OVERVOTE');


--------------------------
--- Military Sites
--------------------------

CREATE TABLE BasesRaw (
    Id bigserial PRIMARY KEY,
    "OBJECTID" TEXT,
    "Country" TEXT,
    "Feature Description" TEXT,
    "Feature Name" TEXT,
    "Controlled Unclassified Information Indicator" TEXT,
    "Is FIRRMA Site" TEXT,
    "Is Joint Base" TEXT,
    "Media Identifier" TEXT,
    "Primary Key Identifier" TEXT,
    "Globally Unique Identifier" TEXT,
    "Site Name" TEXT,
    "Site Operational Status" TEXT,
    "Site Reporting Component Code" TEXT,
    "State Name Code" TEXT,
    "Shape__Area" TEXT,
    "Shape__Length" TEXT
);

COPY BasesRaw (
    "OBJECTID",
    "Country",
    "Feature Description",
    "Feature Name",
    "Controlled Unclassified Information Indicator",
    "Is FIRRMA Site",
    "Is Joint Base",
    "Media Identifier",
    "Primary Key Identifier",
    "Globally Unique Identifier",
    "Site Name",
    "Site Operational Status",
    "Site Reporting Component Code",
    "State Name Code",
    "Shape__Area",
    "Shape__Length"
)
FROM '/private/tmp/bases.csv'
WITH (
    FORMAT csv,
    HEADER true
);

CREATE TABLE Bases (
    Id bigserial PRIMARY KEY,
    SiteName TEXT NOT NULL,
    SiteOperationalStatus TEXT,
    SiteReportingComponentCode TEXT,
    StateId bigint NOT NULL,
    FOREIGN KEY (StateId) REFERENCES States(StateId)
);

CREATE INDEX idx_bases_stateid ON Bases(StateId);
CREATE INDEX idx_bases_name ON Bases(SiteName);

INSERT INTO Bases (
    SiteName,
    SiteOperationalStatus,
    SiteReportingComponentCode,
    StateId
)
SELECT
    TRIM(B."Site Name"),
    CASE
        WHEN TRIM(B."Site Operational Status") = '' THEN NULL
        ELSE TRIM(B."Site Operational Status")
    END,
    CASE
        WHEN TRIM(B."Site Reporting Component Code") = '' THEN NULL
        ELSE TRIM(B."Site Reporting Component Code")
    END,
    S.StateId
FROM BasesRaw B
JOIN States S
    ON S.StateCode = UPPER(TRIM(B."State Name Code"))
WHERE TRIM(B."Site Name") <> ''
  AND TRIM(B."State Name Code") <> '';