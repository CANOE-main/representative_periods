PRAGMA foreign_keys= OFF;
BEGIN TRANSACTION;

CREATE TABLE IF NOT EXISTS MetaData
(
    element TEXT PRIMARY KEY,
    value   INT,
    notes   TEXT
);
REPLACE INTO MetaData
VALUES ('DB_MAJOR', 3, 'DB major version number');
REPLACE INTO MetaData
VALUES ('DB_MINOR', 1, 'DB minor version number');
REPLACE INTO MetaData
VALUES ('days_per_period', 365, 'count of days in each period');

CREATE TABLE IF NOT EXISTS MetaDataReal
(
    element TEXT PRIMARY KEY,
    value   REAL,
    notes   TEXT
);
REPLACE INTO MetaDataReal
VALUES ('global_discount_rate', 0.03, 'Canadian social discount rate');
REPLACE INTO MetaDataReal
VALUES ('default_loan_rate', 0.03, 'Matching GDR');

CREATE TABLE IF NOT EXISTS SeasonLabel
(
    season TEXT
        PRIMARY KEY,
    notes  TEXT
);
CREATE TABLE IF NOT EXISTS SectorLabel
(
    sector TEXT,
    notes  TEXT,
    PRIMARY KEY (sector)
);
CREATE TABLE IF NOT EXISTS CapacityCredit
(
    region  TEXT,
    period  INTEGER
        REFERENCES TimePeriod (period),
    tech    TEXT,
    vintage INTEGER,
    credit  REAL,
    notes   TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    PRIMARY KEY (region, period, tech, vintage),
    CHECK (credit >= 0 AND credit <= 1)
);
CREATE TABLE IF NOT EXISTS CapacityFactorProcess
(
    region  TEXT,
    period  INTEGER
        REFERENCES TimePeriod (period),
    season TEXT
        REFERENCES SeasonLabel (season),
    tod     TEXT
        REFERENCES TimeOfDay (tod),
    tech    TEXT,
    vintage INTEGER,
    factor  REAL,
    notes   TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    PRIMARY KEY (region, period, season, tod, tech, vintage),
    CHECK (factor >= 0 AND factor <= 1)
);
CREATE TABLE IF NOT EXISTS CapacityFactorTech
(
    region TEXT,
    period INTEGER
        REFERENCES TimePeriod (period),
    season TEXT
        REFERENCES SeasonLabel (season),
    tod    TEXT
        REFERENCES TimeOfDay (tod),
    tech   TEXT,
    factor REAL,
    notes  TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    PRIMARY KEY (region, period, season, tod, tech),
    CHECK (factor >= 0 AND factor <= 1)
);
CREATE TABLE IF NOT EXISTS CapacityToActivity
(
    region TEXT,
    tech   TEXT,
    c2a    REAL,
    notes  TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    PRIMARY KEY (region, tech)
);
CREATE TABLE IF NOT EXISTS Commodity
(
    name        TEXT,
    flag        TEXT
        REFERENCES CommodityType (label),
    description TEXT,
    data_id TEXT
        REFERENCES DataSet (data_id),
    PRIMARY KEY (name)
);
CREATE TABLE IF NOT EXISTS CommodityType
(
    label       TEXT PRIMARY KEY,
    description TEXT
);
REPLACE INTO CommodityType
VALUES ('s', 'source commodity');
REPLACE INTO CommodityType
VALUES ('a', 'annual commodity');
REPLACE INTO CommodityType
VALUES ('p', 'physical commodity');
REPLACE INTO CommodityType
VALUES ('d', 'demand commodity');
REPLACE INTO CommodityType
VALUES ('e', 'emissions commodity');
REPLACE INTO CommodityType
VALUES ('w', 'waste commodity');
REPLACE INTO CommodityType
VALUES ('wa', 'waste annual commodity');
REPLACE INTO CommodityType
VALUES ('wp', 'waste physical commodity');
CREATE TABLE IF NOT EXISTS ConstructionInput
(
    region      TEXT,
    input_comm   TEXT,
    tech        TEXT,
    vintage     INTEGER
        REFERENCES TimePeriod (period),
    value       REAL,
    units       TEXT,
    notes       TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    FOREIGN KEY (input_comm) REFERENCES Commodity (name),
    PRIMARY KEY (region, input_comm, tech, vintage)
);
CREATE TABLE IF NOT EXISTS CostEmission
(
    region    TEXT,
    period    INTEGER
        REFERENCES TimePeriod (period),
    emis_comm TEXT NOT NULL,
    cost      REAL NOT NULL,
    units     TEXT,
    notes     TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (emis_comm) REFERENCES Commodity (name),
    PRIMARY KEY (region, period, emis_comm)
);
CREATE TABLE IF NOT EXISTS CostFixed
(
    region  TEXT    NOT NULL,
    period  INTEGER NOT NULL
        REFERENCES TimePeriod (period),
    tech    TEXT    NOT NULL,
    vintage INTEGER NOT NULL
        REFERENCES TimePeriod (period),
    cost    REAL,
    units   TEXT,
    notes   TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    PRIMARY KEY (region, period, tech, vintage)
);
CREATE TABLE IF NOT EXISTS CostInvest
(
    region  TEXT,
    tech    TEXT,
    vintage INTEGER
        REFERENCES TimePeriod (period),
    cost    REAL,
    units   TEXT,
    notes   TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    PRIMARY KEY (region, tech, vintage)
);
CREATE TABLE IF NOT EXISTS CostVariable
(
    region  TEXT    NOT NULL,
    period  INTEGER NOT NULL
        REFERENCES TimePeriod (period),
    tech    TEXT    NOT NULL,
    vintage INTEGER NOT NULL
        REFERENCES TimePeriod (period),
    cost    REAL,
    units   TEXT,
    notes   TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    PRIMARY KEY (region, period, tech, vintage)
);
CREATE TABLE IF NOT EXISTS Demand
(
    region    TEXT,
    period    INTEGER
        REFERENCES TimePeriod (period),
    commodity TEXT,
    demand    REAL,
    units     TEXT,
    notes     TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (commodity) REFERENCES Commodity (name),
    PRIMARY KEY (region, period, commodity)
);
CREATE TABLE IF NOT EXISTS DemandSpecificDistribution
(
    region      TEXT,
    period      INTEGER
        REFERENCES TimePeriod (period),
    season TEXT
        REFERENCES SeasonLabel (season),
    tod         TEXT
        REFERENCES TimeOfDay (tod),
    demand_name TEXT,
    dsd         REAL,
    notes       TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (demand_name) REFERENCES Commodity (name),
    PRIMARY KEY (region, period, season, tod, demand_name),
    CHECK (dsd >= 0 AND dsd <= 1)
);
CREATE TABLE IF NOT EXISTS EndOfLifeOutput
(
    region      TEXT,
    tech        TEXT,
    vintage     INTEGER
        REFERENCES TimePeriod (period),
    output_comm   TEXT,
    value       REAL,
    units       TEXT,
    notes       TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    FOREIGN KEY (output_comm) REFERENCES Commodity (name),
    PRIMARY KEY (region, tech, vintage, output_comm)
);
CREATE TABLE IF NOT EXISTS Efficiency
(
    region      TEXT,
    input_comm  TEXT,
    tech        TEXT,
    vintage     INTEGER
        REFERENCES TimePeriod (period),
    output_comm TEXT,
    efficiency  REAL,
    notes       TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    FOREIGN KEY (input_comm) REFERENCES Commodity (name),
    FOREIGN KEY (output_comm) REFERENCES Commodity (name),
    PRIMARY KEY (region, input_comm, tech, vintage, output_comm),
    CHECK (efficiency > 0)
);
CREATE TABLE IF NOT EXISTS EfficiencyVariable
(
    region      TEXT,
    period      INTEGER
        REFERENCES TimePeriod (period),
    season TEXT
        REFERENCES SeasonLabel (season),
    tod         TEXT
        REFERENCES TimeOfDay (tod),
    input_comm  TEXT,
    tech        TEXT,
    vintage     INTEGER
        REFERENCES TimePeriod (period),
    output_comm TEXT,
    efficiency  REAL,
    notes       TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    FOREIGN KEY (input_comm) REFERENCES Commodity (name),
    FOREIGN KEY (output_comm) REFERENCES Commodity (name),
    PRIMARY KEY (region, period, season, tod, input_comm, tech, vintage, output_comm),
    CHECK (efficiency > 0)
);
CREATE TABLE IF NOT EXISTS EmissionActivity
(
    region      TEXT,
    emis_comm   TEXT,
    input_comm  TEXT,
    tech        TEXT,
    vintage     INTEGER
        REFERENCES TimePeriod (period),
    output_comm TEXT,
    activity    REAL,
    units       TEXT,
    notes       TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    FOREIGN KEY (emis_comm) REFERENCES Commodity (name),
    FOREIGN KEY (input_comm) REFERENCES Commodity (name),
    FOREIGN KEY (output_comm) REFERENCES Commodity (name),
    PRIMARY KEY (region, emis_comm, input_comm, tech, vintage, output_comm)
);
CREATE TABLE IF NOT EXISTS EmissionEmbodied
(
    region      TEXT,
    emis_comm   TEXT,
    tech        TEXT,
    vintage     INTEGER
        REFERENCES TimePeriod (period),
    value       REAL,
    units       TEXT,
    notes       TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    FOREIGN KEY (emis_comm) REFERENCES Commodity (name),
    PRIMARY KEY (region, emis_comm,  tech, vintage)
);
CREATE TABLE IF NOT EXISTS EmissionEndOfLife
(
    region      TEXT,
    emis_comm   TEXT,
    tech        TEXT,
    vintage     INTEGER
        REFERENCES TimePeriod (period),
    value       REAL,
    units       TEXT,
    notes       TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    FOREIGN KEY (emis_comm) REFERENCES Commodity (name),
    PRIMARY KEY (region, emis_comm,  tech, vintage)
);
CREATE TABLE IF NOT EXISTS ExistingCapacity
(
    region   TEXT,
    tech     TEXT,
    vintage  INTEGER
        REFERENCES TimePeriod (period),
    capacity REAL,
    units    TEXT,
    notes    TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    PRIMARY KEY (region, tech, vintage)
);
CREATE TABLE IF NOT EXISTS TechGroup
(
    group_name TEXT,
    notes      TEXT,
    data_id TEXT
        REFERENCES DataSet (data_id),
    PRIMARY KEY (group_name)
);
CREATE TABLE IF NOT EXISTS LoanLifetimeProcess
(
    region   TEXT,
    tech     TEXT,
    vintage  INTEGER
        REFERENCES TimePeriod (period),
    lifetime REAL,
    notes    TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    PRIMARY KEY (region, tech, vintage)
);
CREATE TABLE IF NOT EXISTS LoanRate
(
    region  TEXT,
    tech    TEXT,
    vintage INTEGER
        REFERENCES TimePeriod (period),
    rate    REAL,
    notes   TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    PRIMARY KEY (region, tech, vintage)
);
CREATE TABLE IF NOT EXISTS LifetimeProcess
(
    region   TEXT,
    tech     TEXT,
    vintage  INTEGER
        REFERENCES TimePeriod (period),
    lifetime REAL,
    notes    TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    PRIMARY KEY (region, tech, vintage)
);
CREATE TABLE IF NOT EXISTS LifetimeTech
(
    region   TEXT,
    tech     TEXT,
    lifetime REAL,
    notes    TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    PRIMARY KEY (region, tech)
);
CREATE TABLE IF NOT EXISTS Operator
(
	operator TEXT PRIMARY KEY,
	notes TEXT
);
REPLACE INTO Operator VALUES('e','equal to');
REPLACE INTO Operator VALUES('le','less than or equal to');
REPLACE INTO Operator VALUES('ge','greater than or equal to');
CREATE TABLE IF NOT EXISTS OutputDualVariable
(
    scenario        TEXT,
    constraint_name TEXT,
    dual            REAL,
    PRIMARY KEY (constraint_name, scenario)
);
CREATE TABLE IF NOT EXISTS OutputObjective
(
    scenario          TEXT,
    objective_name    TEXT,
    total_system_cost REAL
);
CREATE TABLE IF NOT EXISTS OutputCurtailment
(
    scenario    TEXT,
    region      TEXT,
    sector      TEXT,
    period      INTEGER
        REFERENCES TimePeriod (period),
    season      TEXT
        REFERENCES TimePeriod (period),
    tod         TEXT
        REFERENCES TimeOfDay (tod),
    input_comm  TEXT
        REFERENCES Commodity (name),
    tech        TEXT
        REFERENCES Technology (tech),
    vintage     INTEGER
        REFERENCES TimePeriod (period),
    output_comm TEXT
        REFERENCES Commodity (name),
    curtailment REAL,
    PRIMARY KEY (region, scenario, period, season, tod, input_comm, tech, vintage, output_comm)
);
CREATE TABLE IF NOT EXISTS OutputNetCapacity
(
    scenario TEXT,
    region   TEXT,
    sector   TEXT
        REFERENCES SectorLabel (sector),
    period   INTEGER
        REFERENCES TimePeriod (period),
    tech     TEXT
        REFERENCES Technology (tech),
    vintage  INTEGER
        REFERENCES TimePeriod (period),
    capacity REAL,
    PRIMARY KEY (region, scenario, period, tech, vintage)
);
CREATE TABLE IF NOT EXISTS OutputBuiltCapacity
(
    scenario TEXT,
    region   TEXT,
    sector   TEXT
        REFERENCES SectorLabel (sector),
    tech     TEXT
        REFERENCES Technology (tech),
    vintage  INTEGER
        REFERENCES TimePeriod (period),
    capacity REAL,
    PRIMARY KEY (region, scenario, tech, vintage)
);
CREATE TABLE IF NOT EXISTS OutputRetiredCapacity
(
    scenario TEXT,
    region   TEXT,
    sector   TEXT
        REFERENCES SectorLabel (sector),
    period   INTEGER
        REFERENCES TimePeriod (period),
    tech     TEXT
        REFERENCES Technology (tech),
    vintage  INTEGER
        REFERENCES TimePeriod (period),
    cap_eol REAL,
    cap_early REAL,
    PRIMARY KEY (region, scenario, period, tech, vintage)
);
CREATE TABLE IF NOT EXISTS OutputFlowIn
(
    scenario    TEXT,
    region      TEXT,
    sector      TEXT
        REFERENCES SectorLabel (sector),
    period      INTEGER
        REFERENCES TimePeriod (period),
    season TEXT
        REFERENCES SeasonLabel (season),
    tod         TEXT
        REFERENCES TimeOfDay (tod),
    input_comm  TEXT
        REFERENCES Commodity (name),
    tech        TEXT
        REFERENCES Technology (tech),
    vintage     INTEGER
        REFERENCES TimePeriod (period),
    output_comm TEXT
        REFERENCES Commodity (name),
    flow        REAL,
    PRIMARY KEY (region, scenario, period, season, tod, input_comm, tech, vintage, output_comm)
);
CREATE TABLE IF NOT EXISTS OutputFlowOut
(
    scenario    TEXT,
    region      TEXT,
    sector      TEXT
        REFERENCES SectorLabel (sector),
    period      INTEGER
        REFERENCES TimePeriod (period),
    season TEXT
        REFERENCES SeasonLabel (season),
    tod         TEXT
        REFERENCES TimeOfDay (tod),
    input_comm  TEXT
        REFERENCES Commodity (name),
    tech        TEXT
        REFERENCES Technology (tech),
    vintage     INTEGER
        REFERENCES TimePeriod (period),
    output_comm TEXT
        REFERENCES Commodity (name),
    flow        REAL,
    PRIMARY KEY (region, scenario, period, season, tod, input_comm, tech, vintage, output_comm)
);
CREATE TABLE IF NOT EXISTS OutputStorageLevel
(
    scenario TEXT,
    region TEXT,
    sector TEXT
        REFERENCES SectorLabel (sector),
    period INTEGER
        REFERENCES TimePeriod (period),
    season TEXT
        REFERENCES SeasonLabel (season),
    tod TEXT
        REFERENCES TimeOfDay (tod),
    tech TEXT
        REFERENCES Technology (tech),
    vintage INTEGER
        REFERENCES TimePeriod (period),
    level REAL,
    PRIMARY KEY (scenario, region, period, season, tod, tech, vintage)
);
CREATE TABLE IF NOT EXISTS OutputEmission
(
    scenario  TEXT,
    region    TEXT,
    sector    TEXT
        REFERENCES SectorLabel (sector),
    period    INTEGER
        REFERENCES TimePeriod (period),
    emis_comm TEXT
        REFERENCES Commodity (name),
    tech      TEXT
        REFERENCES Technology (tech),
    vintage   INTEGER
        REFERENCES TimePeriod (period),
    emission  REAL,
    PRIMARY KEY (region, scenario, period, emis_comm, tech, vintage)
);
CREATE TABLE IF NOT EXISTS OutputCost
(
    scenario TEXT,
    region   TEXT,
    sector   TEXT REFERENCES SectorLabel (sector),
    period   INTEGER REFERENCES TimePeriod (period),
    tech     TEXT REFERENCES Technology (tech),
    vintage  INTEGER REFERENCES TimePeriod (period),
    d_invest REAL,
    d_fixed  REAL,
    d_var    REAL,
    d_emiss  REAL,
    invest   REAL,
    fixed    REAL,
    var      REAL,
    emiss    REAL,
    PRIMARY KEY (scenario, region, period, tech, vintage),
    FOREIGN KEY (vintage) REFERENCES TimePeriod (period),
    FOREIGN KEY (tech) REFERENCES Technology (tech)
);
CREATE TABLE IF NOT EXISTS LimitGrowthCapacity
(
    region TEXT,
    tech_or_group   TEXT,
    operator TEXT NOT NULL DEFAULT "le"
    	REFERENCES Operator (operator),
    rate   REAL NOT NULL DEFAULT 0,
    seed   REAL NOT NULL DEFAULT 0,
    seed_units TEXT,
    notes  TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    PRIMARY KEY (region, tech_or_group, operator)
);
CREATE TABLE IF NOT EXISTS LimitDegrowthCapacity
(
    region TEXT,
    tech_or_group   TEXT,
    operator TEXT NOT NULL DEFAULT "le"
    	REFERENCES Operator (operator),
    rate   REAL NOT NULL DEFAULT 0,
    seed   REAL NOT NULL DEFAULT 0,
    seed_units TEXT,
    notes  TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    PRIMARY KEY (region, tech_or_group, operator)
);
CREATE TABLE IF NOT EXISTS LimitGrowthNewCapacity
(
    region TEXT,
    tech_or_group   TEXT,
    operator TEXT NOT NULL DEFAULT "le"
    	REFERENCES Operator (operator),
    rate   REAL NOT NULL DEFAULT 0,
    seed   REAL NOT NULL DEFAULT 0,
    seed_units TEXT,
    notes  TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    PRIMARY KEY (region, tech_or_group, operator)
);
CREATE TABLE IF NOT EXISTS LimitDegrowthNewCapacity
(
    region TEXT,
    tech_or_group   TEXT,
    operator TEXT NOT NULL DEFAULT "le"
    	REFERENCES Operator (operator),
    rate   REAL NOT NULL DEFAULT 0,
    seed   REAL NOT NULL DEFAULT 0,
    seed_units TEXT,
    notes  TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    PRIMARY KEY (region, tech_or_group, operator)
);
CREATE TABLE IF NOT EXISTS LimitGrowthNewCapacityDelta
(
    region TEXT,
    tech_or_group   TEXT,
    operator TEXT NOT NULL DEFAULT "le"
    	REFERENCES Operator (operator),
    rate   REAL NOT NULL DEFAULT 0,
    seed   REAL NOT NULL DEFAULT 0,
    seed_units TEXT,
    notes  TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    PRIMARY KEY (region, tech_or_group, operator)
);
CREATE TABLE IF NOT EXISTS LimitDegrowthNewCapacityDelta
(
    region TEXT,
    tech_or_group   TEXT,
    operator TEXT NOT NULL DEFAULT "le"
    	REFERENCES Operator (operator),
    rate   REAL NOT NULL DEFAULT 0,
    seed   REAL NOT NULL DEFAULT 0,
    seed_units TEXT,
    notes  TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    PRIMARY KEY (region, tech_or_group, operator)
);
CREATE TABLE IF NOT EXISTS LimitStorageLevelFraction
(
    region   TEXT,
    period   INTEGER
        REFERENCES TimePeriod (period),
    season TEXT
        REFERENCES SeasonLabel (season),
    tod      TEXT
        REFERENCES TimeOfDay (tod),
    tech     TEXT,
    vintage  INTEGER
        REFERENCES TimePeriod (period),
    operator	TEXT  NOT NULL DEFAULT "le"
    	REFERENCES Operator (operator),
    fraction REAL,
    notes    TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    PRIMARY KEY (region, period, season, tod, tech, vintage, operator)
);
CREATE TABLE IF NOT EXISTS LimitActivity
(
    region  TEXT,
    period  INTEGER
        REFERENCES TimePeriod (period),
    tech_or_group   TEXT,
    operator	TEXT  NOT NULL DEFAULT "le"
    	REFERENCES Operator (operator),
    activity REAL,
    units   TEXT,
    notes   TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    PRIMARY KEY (region, period, tech_or_group, operator)
);
CREATE TABLE IF NOT EXISTS LimitActivityShare
(
    region         TEXT,
    period         INTEGER
        REFERENCES TimePeriod (period),
    sub_group      TEXT,
    super_group    TEXT,
    operator	TEXT  NOT NULL DEFAULT "le"
    	REFERENCES Operator (operator),
    share REAL,
    notes          TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    PRIMARY KEY (region, period, sub_group, super_group, operator)
);
CREATE TABLE IF NOT EXISTS LimitAnnualCapacityFactor
(
    region      TEXT,
    period      INTEGER
        REFERENCES TimePeriod (period),
    tech        TEXT,
    output_comm TEXT,
    operator	TEXT  NOT NULL DEFAULT "le"
    	REFERENCES Operator (operator),
    factor      REAL,
    notes       TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    FOREIGN KEY (output_comm) REFERENCES Commodity (name),
    PRIMARY KEY (region, period, tech, output_comm, operator),
    CHECK (factor >= 0 AND factor <= 1)
);
CREATE TABLE IF NOT EXISTS LimitCapacity
(
    region  TEXT,
    period  INTEGER
        REFERENCES TimePeriod (period),
    tech_or_group   TEXT,
    operator	TEXT  NOT NULL DEFAULT "le"
    	REFERENCES Operator (operator),
    capacity REAL,
    units   TEXT,
    notes   TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    PRIMARY KEY (region, period, tech_or_group, operator)
);
CREATE TABLE IF NOT EXISTS LimitCapacityShare
(
    region         TEXT,
    period         INTEGER
        REFERENCES TimePeriod (period),
    sub_group      TEXT,
    super_group    TEXT,
    operator	TEXT  NOT NULL DEFAULT "le"
    	REFERENCES Operator (operator),
    share REAL,
    notes          TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    PRIMARY KEY (region, period, sub_group, super_group, operator)
);
CREATE TABLE IF NOT EXISTS LimitNewCapacity
(
    region  TEXT,
    period  INTEGER
        REFERENCES TimePeriod (period),
    tech_or_group   TEXT,
    operator	TEXT  NOT NULL DEFAULT "le"
    	REFERENCES Operator (operator),
    new_cap REAL,
    units   TEXT,
    notes   TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    PRIMARY KEY (region, period, tech_or_group, operator)
);
CREATE TABLE IF NOT EXISTS LimitNewCapacityShare
(
    region         TEXT,
    period         INTEGER
        REFERENCES TimePeriod (period),
    sub_group      TEXT,
    super_group    TEXT,
    operator	TEXT  NOT NULL DEFAULT "le"
    	REFERENCES Operator (operator),
    share REAL,
    notes          TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    PRIMARY KEY (region, period, sub_group, super_group, operator)
);
CREATE TABLE IF NOT EXISTS LimitResource
(
    region  TEXT,
    tech_or_group   TEXT,
    operator	TEXT  NOT NULL DEFAULT "le"
    	REFERENCES Operator (operator),
    cum_act REAL,
    units   TEXT,
    notes   TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    PRIMARY KEY (region, tech_or_group, operator)
);
CREATE TABLE IF NOT EXISTS LimitSeasonalCapacityFactor
(
	region  TEXT,
	period	INTEGER
        REFERENCES TimePeriod (period),
	season TEXT
        REFERENCES SeasonLabel (season),
	tech    TEXT,
    operator	TEXT  NOT NULL DEFAULT "le"
    	REFERENCES Operator (operator),
	factor	REAL,
	notes	TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (region) REFERENCES Region (region),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
	PRIMARY KEY (region, period, season, tech, operator)
);
CREATE TABLE IF NOT EXISTS LimitTechInputSplit
(
    region         TEXT,
    period         INTEGER
        REFERENCES TimePeriod (period),
    input_comm     TEXT,
    tech           TEXT,
    operator	TEXT  NOT NULL DEFAULT "le"
    	REFERENCES Operator (operator),
    proportion REAL,
    notes          TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    FOREIGN KEY (input_comm) REFERENCES Commodity (name),
    PRIMARY KEY (region, period, input_comm, tech, operator)
);
CREATE TABLE IF NOT EXISTS LimitTechInputSplitAnnual
(
    region         TEXT,
    period         INTEGER
        REFERENCES TimePeriod (period),
    input_comm     TEXT,
    tech           TEXT,
    operator	TEXT  NOT NULL DEFAULT "le"
    	REFERENCES Operator (operator),
    proportion REAL,
    notes          TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    PRIMARY KEY (region, period, input_comm, tech, operator)
);
CREATE TABLE IF NOT EXISTS LimitTechOutputSplit
(
    region         TEXT,
    period         INTEGER
        REFERENCES TimePeriod (period),
    tech           TEXT,
    output_comm    TEXT,
    operator	TEXT  NOT NULL DEFAULT "le"
    	REFERENCES Operator (operator),
    proportion REAL,
    notes          TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    FOREIGN KEY (output_comm) REFERENCES Commodity (name),
    PRIMARY KEY (region, period, tech, output_comm, operator)
);
CREATE TABLE IF NOT EXISTS LimitTechOutputSplitAnnual
(
    region         TEXT,
    period         INTEGER
        REFERENCES TimePeriod (period),
    tech           TEXT,
    output_comm    TEXT,
    operator	TEXT  NOT NULL DEFAULT "le"
    	REFERENCES Operator (operator),
    proportion REAL,
    notes          TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    FOREIGN KEY (output_comm) REFERENCES Commodity (name),
    PRIMARY KEY (region, period, tech, output_comm, operator)
);
CREATE TABLE IF NOT EXISTS LimitEmission
(
    region    TEXT,
    period    INTEGER
        REFERENCES TimePeriod (period),
    emis_comm TEXT,
    operator	TEXT  NOT NULL DEFAULT "le"
    	REFERENCES Operator (operator),
    value     REAL,
    units     TEXT,
    notes     TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (emis_comm) REFERENCES Commodity (name),
    PRIMARY KEY (region, period, emis_comm, operator)
);
CREATE TABLE IF NOT EXISTS LinkedTech
(
    primary_region TEXT,
    primary_tech   TEXT,
    emis_comm      TEXT,
    driven_tech    TEXT,
    notes          TEXT,
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (primary_tech) REFERENCES Technology (tech),
    FOREIGN KEY (driven_tech) REFERENCES Technology (tech),
    FOREIGN KEY (emis_comm) REFERENCES Commodity (name),
    PRIMARY KEY (primary_region, primary_tech, emis_comm)
);
CREATE TABLE IF NOT EXISTS PlanningReserveMargin
(
    region TEXT,
    margin REAL,
    notes TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (region) REFERENCES Region (region),
    PRIMARY KEY (region)
);
CREATE TABLE IF NOT EXISTS RampDownHourly
(
    region TEXT,
    tech   TEXT,
    rate   REAL,
    notes TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    PRIMARY KEY (region, tech)
);
CREATE TABLE IF NOT EXISTS RampUpHourly
(
    region TEXT,
    tech   TEXT,
    rate   REAL,
    notes TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    PRIMARY KEY (region, tech)
);
CREATE TABLE IF NOT EXISTS Region
(
    region TEXT,
    notes  TEXT,
    PRIMARY KEY (region)
);
CREATE TABLE IF NOT EXISTS ReserveCapacityDerate
(
    region  TEXT,
    period  INTEGER
        REFERENCES TimePeriod (period),
    season  TEXT
    	REFERENCES SeasonLabel (season),
    tech    TEXT,
    vintage INTEGER,
    factor  REAL,
    notes   TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    PRIMARY KEY (region, period, season, tech, vintage),
    CHECK (factor >= 0 AND factor <= 1)
);
CREATE TABLE IF NOT EXISTS TimeSegmentFraction
(   
    period INTEGER
        REFERENCES TimePeriod (period),
    season TEXT
        REFERENCES SeasonLabel (season),
    tod     TEXT
        REFERENCES TimeOfDay (tod),
    segfrac REAL,
    notes   TEXT,
    PRIMARY KEY (period, season, tod),
    CHECK (segfrac >= 0 AND segfrac <= 1)
);
CREATE TABLE IF NOT EXISTS StorageDuration
(
    region   TEXT,
    tech     TEXT,
    duration REAL,
    notes    TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    PRIMARY KEY (region, tech)
);
CREATE TABLE IF NOT EXISTS LifetimeSurvivalCurve
(
    region  TEXT    NOT NULL,
    period  INTEGER NOT NULL,
    tech    TEXT    NOT NULL,
    vintage INTEGER NOT NULL
        REFERENCES TimePeriod (period),
    fraction  REAL,
    notes   TEXT,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    PRIMARY KEY (region, period, tech, vintage)
);
CREATE TABLE IF NOT EXISTS TechnologyType
(
    label       TEXT PRIMARY KEY,
    description TEXT
);
REPLACE INTO TechnologyType
VALUES ('p', 'production technology');
REPLACE INTO TechnologyType
VALUES ('pb', 'baseload production technology');
REPLACE INTO TechnologyType
VALUES ('ps', 'storage production technology');
-- CREATE TABLE IF NOT EXISTS TimeNext
-- (
--     period       INTEGER
--         REFERENCES TimePeriod (period),
--     season TEXT
--        REFERENCES SeasonLabel (season),
--     tod          TEXT
--         REFERENCES TimeOfDay (tod),
--     season_next TEXT
--        REFERENCES SeasonLabel (season),
--     tod_next     TEXT
--         REFERENCES TimeOfDay (tod),
--     notes        TEXT,
--     PRIMARY KEY (period, season, tod)
-- );
CREATE TABLE IF NOT EXISTS TimeOfDay
(
    sequence INTEGER UNIQUE,
    tod      TEXT
        PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS TimePeriod
(
    sequence INTEGER UNIQUE,
    period   INTEGER
        PRIMARY KEY,
    flag     TEXT
        REFERENCES TimePeriodType (label)
);
CREATE TABLE IF NOT EXISTS TimeSeason
(
    period INTEGER
        REFERENCES TimePeriod (period),
    sequence INTEGER,
    season TEXT
        REFERENCES SeasonLabel (season),
    notes TEXT,
    PRIMARY KEY (period, sequence, season)
);
CREATE TABLE IF NOT EXISTS TimeSeasonSequential
(
    period INTEGER
        REFERENCES TimePeriod (period),
    sequence INTEGER,
    seas_seq TEXT,
    season TEXT
        REFERENCES SeasonLabel (season),
    num_days REAL NOT NULL,
    notes TEXT,
    PRIMARY KEY (period, sequence, seas_seq, season),
    CHECK (num_days > 0)
);
CREATE TABLE IF NOT EXISTS TimePeriodType
(
    label       TEXT PRIMARY KEY,
    description TEXT
);
REPLACE INTO TimePeriodType
VALUES('e', 'existing vintages');
REPLACE INTO TimePeriodType
VALUES('f', 'future');
CREATE TABLE IF NOT EXISTS RPSRequirement
(
    region      TEXT    NOT NULL,
    period      INTEGER NOT NULL
        REFERENCES TimePeriod (period),
    tech_group  TEXT    NOT NULL,
    requirement REAL    NOT NULL,
    data_source TEXT,
    dq_cred INTEGER
        REFERENCES DataQualityCredibility (dq_cred),
    dq_geog INTEGER
        REFERENCES DataQualityGeography (dq_geog),
    dq_struc INTEGER
        REFERENCES DataQualityStructure (dq_struc),
    dq_tech INTEGER
        REFERENCES DataQualityTechnology (dq_tech),
    dq_time INTEGER
        REFERENCES DataQualityTime (dq_time),
    data_id TEXT
        REFERENCES DataSet (data_id),
    notes       TEXT,
    FOREIGN KEY (data_source) REFERENCES DataSource (source_id),
    FOREIGN KEY (region) REFERENCES Region (region),
    FOREIGN KEY (tech_group) REFERENCES TechGroup (group_name),
    PRIMARY KEY (region)
);
CREATE TABLE IF NOT EXISTS TechGroupMember
(
    group_name TEXT,
    tech       TEXT,
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (tech) REFERENCES Technology (tech),
    FOREIGN KEY (group_name) REFERENCES TechGroup (group_name),
    PRIMARY KEY (group_name, tech)
);
CREATE TABLE IF NOT EXISTS Technology
(
    tech         TEXT    NOT NULL,
    flag         TEXT    NOT NULL,
    sector       TEXT,
    category     TEXT,
    sub_category TEXT,
    unlim_cap    INTEGER NOT NULL DEFAULT 0,
    annual       INTEGER NOT NULL DEFAULT 0,
    reserve      INTEGER NOT NULL DEFAULT 0,
    curtail      INTEGER NOT NULL DEFAULT 0,
    retire       INTEGER NOT NULL DEFAULT 0,
    flex         INTEGER NOT NULL DEFAULT 0,
    exchange     INTEGER NOT NULL DEFAULT 0,
    seas_stor    INTEGER NOT NULL DEFAULT 0,
    description  TEXT,
    data_id TEXT
        REFERENCES DataSet (data_id),
    FOREIGN KEY (flag) REFERENCES TechnologyType (label),
    PRIMARY KEY (tech)
);
CREATE TABLE IF NOT EXISTS DataSource
(
    source_id TEXT,
    source TEXT,
    notes TEXT,
    data_id TEXT
        REFERENCES DataSet (data_id),
    PRIMARY KEY (source_id)
);
CREATE TABLE IF NOT EXISTS DataQualityCredibility
(
    dq_cred INTEGER PRIMARY KEY,
    description TEXT
);
REPLACE INTO DataQualityCredibility VALUES (1,'Excellent - A trustworthy source backed by strong analysis or direct measurements.');
REPLACE INTO DataQualityCredibility VALUES (2,'Good - Trustworthy source. Partly based on assumptions or imperfect analysis.');
REPLACE INTO DataQualityCredibility VALUES (3,'Acceptable - Acceptable source. May rely on many assumptions, shallow analysis, or rough measurement.');
REPLACE INTO DataQualityCredibility VALUES (4,'Lacking - Questionable or unverified source. Poorly measured or weak analysis.');
REPLACE INTO DataQualityCredibility VALUES (5,'Unacceptable - No or untrustworthy source. Unsupported assumption.');
CREATE TABLE IF NOT EXISTS DataQualityGeography
(
    dq_geog INTEGER PRIMARY KEY,
    description TEXT
);
REPLACE INTO DataQualityGeography VALUES (1,'Excellent - From this region and at the correct aggregation level or a directly-applicable generic value.');
REPLACE INTO DataQualityGeography VALUES (2,'Good - From an analogous region or the modelled region at incorrect aggregation level.');
REPLACE INTO DataQualityGeography VALUES (3,'Acceptable - From a relevant but non-analogous region or highly aggregated.');
REPLACE INTO DataQualityGeography VALUES (4,'Lacking - From a non-analogous region with limited relevance or a generic global value.');
REPLACE INTO DataQualityGeography VALUES (5,'Unacceptable - From a region that is highly dissimilar to the modelled region, or from an unknown region.');
CREATE TABLE IF NOT EXISTS DataQualityStructure
(
    dq_struc INTEGER PRIMARY KEY,
    description TEXT
);
REPLACE INTO DataQualityStructure VALUES (1,'Excellent - Excellent representation of the system, as good or better than other models.');
REPLACE INTO DataQualityStructure VALUES (2,'Good - Well modelled, in line with what others are doing.');
REPLACE INTO DataQualityStructure VALUES (3,'Acceptable - Room for improved representation but works for now.');
REPLACE INTO DataQualityStructure VALUES (4,'Lacking - Poorly represented, overly simplified.');
REPLACE INTO DataQualityStructure VALUES (5,'Unacceptable - Placeholder or dummy representation. Essentially not represented.');
CREATE TABLE IF NOT EXISTS DataQualityTechnology
(
    dq_tech INTEGER PRIMARY KEY,
    description TEXT
);
REPLACE INTO DataQualityTechnology VALUES (1,'Excellent - For the modelled technology as represented. Directly applicable.');
REPLACE INTO DataQualityTechnology VALUES (2,'Good - For the same general technology but not perfectly representative.');
REPLACE INTO DataQualityTechnology VALUES (3,'Acceptable - For an analogous technology. Possibly a subset or general class. Roughly applicable.');
REPLACE INTO DataQualityTechnology VALUES (4,'Lacking - Loosely representative. A niche subset or overbroad general class of the technology.');
REPLACE INTO DataQualityTechnology VALUES (5,'Unacceptable - For a dissimilar or unknown technology. Unknown or poor applicability.');
CREATE TABLE IF NOT EXISTS DataQualityTime
(
    dq_time INTEGER PRIMARY KEY,
    description TEXT
);
REPLACE INTO DataQualityTime VALUES (1,'Excellent - From or directly applicable to the modelled time.');
REPLACE INTO DataQualityTime VALUES (2,'Good - From a different but similar time or only slightly out of date. Still highly relevant.');
REPLACE INTO DataQualityTime VALUES (3,'Acceptable - From a somewhat similar time or several years out of date but still relevant.');
REPLACE INTO DataQualityTime VALUES (4,'Lacking - From a time with different conditions or significantly out of date. Questionable relevance.');
REPLACE INTO DataQualityTime VALUES (5,'Unacceptable - From an irrelevant time or badly out of date.');
CREATE TABLE IF NOT EXISTS DataSet
(
    data_id TEXT PRIMARY KEY,
    label TEXT,
    version TEXT,
    description TEXT,
    status TEXT,
    author TEXT,
    date TEXT,
    parent_id TEXT
        REFERENCES DataSet (data_id),
    changelog TEXT,
    notes TEXT
);

COMMIT;
PRAGMA FOREIGN_KEYS = 1;