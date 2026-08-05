PRAGMA foreign_keys = OFF;
BEGIN TRANSACTION;

-- ============================================================
-- Metadata
-- ============================================================

CREATE TABLE IF NOT EXISTS metadata
(
    element TEXT PRIMARY KEY,
    value   INT,
    notes   TEXT
);
REPLACE INTO metadata VALUES ('DB_MAJOR', 4, 'DB major version number');
REPLACE INTO metadata VALUES ('DB_MINOR', 0, 'DB minor version number');

CREATE TABLE IF NOT EXISTS metadata_real
(
    element TEXT PRIMARY KEY,
    value   REAL,
    notes   TEXT
);
REPLACE INTO metadata_real VALUES ('global_discount_rate', 0.03, 'Discount Rate for future costs');
REPLACE INTO metadata_real VALUES ('default_loan_rate', 0.03, 'Default Loan Rate if not specified in loan_rate table');

-- ============================================================
-- Label / registry tables
-- ============================================================
CREATE TABLE IF NOT EXISTS commodity_label
(
    commodity TEXT PRIMARY KEY,
    notes     TEXT
);

CREATE TABLE IF NOT EXISTS technology_label
(
    tech  TEXT PRIMARY KEY,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS tech_group_label
(
    group_name TEXT PRIMARY KEY,
    notes      TEXT
);

CREATE TABLE IF NOT EXISTS sector_label
(
    sector TEXT PRIMARY KEY,
    notes  TEXT
);

-- ============================================================
-- Enum / type tables
-- ============================================================
CREATE TABLE IF NOT EXISTS commodity_type
(
    label       TEXT PRIMARY KEY,
    description TEXT
);
REPLACE INTO commodity_type VALUES ('s',  'source commodity');
REPLACE INTO commodity_type VALUES ('a',  'annual commodity');
REPLACE INTO commodity_type VALUES ('p',  'physical commodity');
REPLACE INTO commodity_type VALUES ('d',  'demand commodity');
REPLACE INTO commodity_type VALUES ('e',  'emissions commodity');
REPLACE INTO commodity_type VALUES ('w',  'waste commodity');
REPLACE INTO commodity_type VALUES ('wa', 'waste annual commodity');
REPLACE INTO commodity_type VALUES ('wp', 'waste physical commodity');

CREATE TABLE IF NOT EXISTS technology_type
(
    label       TEXT PRIMARY KEY,
    description TEXT
);
REPLACE INTO technology_type VALUES ('p',  'production technology');
REPLACE INTO technology_type VALUES ('pb', 'baseload production technology');
REPLACE INTO technology_type VALUES ('ps', 'storage production technology');

CREATE TABLE IF NOT EXISTS time_period_type
(
    label       TEXT PRIMARY KEY,
    description TEXT
);
REPLACE INTO time_period_type VALUES ('e', 'existing vintages');
REPLACE INTO time_period_type VALUES ('f', 'future');

CREATE TABLE IF NOT EXISTS operator
(
    operator TEXT PRIMARY KEY,
    notes    TEXT
);
REPLACE INTO operator VALUES ('e',  'equal to');
REPLACE INTO operator VALUES ('le', 'less than or equal to');
REPLACE INTO operator VALUES ('ge', 'greater than or equal to');

-- ============================================================
-- Data quality and data source tables (custom)
-- ============================================================
CREATE TABLE IF NOT EXISTS data_quality_credibility
(
    dq_cred     INTEGER PRIMARY KEY,
    description TEXT
);
REPLACE INTO data_quality_credibility VALUES (1, 'Excellent - A trustworthy source backed by strong analysis or direct measurements.');
REPLACE INTO data_quality_credibility VALUES (2, 'Good - Trustworthy source. Partly based on assumptions or imperfect analysis.');
REPLACE INTO data_quality_credibility VALUES (3, 'Acceptable - Acceptable source. May rely on many assumptions, shallow analysis, or rough measurement.');
REPLACE INTO data_quality_credibility VALUES (4, 'Lacking - Questionable or unverified source. Poorly measured or weak analysis.');
REPLACE INTO data_quality_credibility VALUES (5, 'Unacceptable - No or untrustworthy source. Unsupported assumption.');

CREATE TABLE IF NOT EXISTS data_quality_geography
(
    dq_geog     INTEGER PRIMARY KEY,
    description TEXT
);
REPLACE INTO data_quality_geography VALUES (1, 'Excellent - From this region and at the correct aggregation level or a directly-applicable generic value.');
REPLACE INTO data_quality_geography VALUES (2, 'Good - From an analogous region or the modelled region at incorrect aggregation level.');
REPLACE INTO data_quality_geography VALUES (3, 'Acceptable - From a relevant but non-analogous region or highly aggregated.');
REPLACE INTO data_quality_geography VALUES (4, 'Lacking - From a non-analogous region with limited relevance or a generic global value.');
REPLACE INTO data_quality_geography VALUES (5, 'Unacceptable - From a region that is highly dissimilar to the modelled region, or from an unknown region.');

CREATE TABLE IF NOT EXISTS data_quality_structure
(
    dq_struc    INTEGER PRIMARY KEY,
    description TEXT
);
REPLACE INTO data_quality_structure VALUES (1, 'Excellent - Excellent representation of the system, as good or better than other models.');
REPLACE INTO data_quality_structure VALUES (2, 'Good - Well modelled, in line with what others are doing.');
REPLACE INTO data_quality_structure VALUES (3, 'Acceptable - Room for improved representation but works for now.');
REPLACE INTO data_quality_structure VALUES (4, 'Lacking - Poorly represented, overly simplified.');
REPLACE INTO data_quality_structure VALUES (5, 'Unacceptable - Placeholder or dummy representation. Essentially not represented.');

CREATE TABLE IF NOT EXISTS data_quality_technology
(
    dq_tech     INTEGER PRIMARY KEY,
    description TEXT
);
REPLACE INTO data_quality_technology VALUES (1, 'Excellent - For the modelled technology as represented. Directly applicable.');
REPLACE INTO data_quality_technology VALUES (2, 'Good - For the same general technology but not perfectly representative.');
REPLACE INTO data_quality_technology VALUES (3, 'Acceptable - For an analogous technology. Possibly a subset or general class. Roughly applicable.');
REPLACE INTO data_quality_technology VALUES (4, 'Lacking - Loosely representative. A niche subset or overbroad general class of the technology.');
REPLACE INTO data_quality_technology VALUES (5, 'Unacceptable - For a dissimilar or unknown technology. Unknown or poor applicability.');

CREATE TABLE IF NOT EXISTS data_quality_time
(
    dq_time     INTEGER PRIMARY KEY,
    description TEXT
);
REPLACE INTO data_quality_time VALUES (1, 'Excellent - From or directly applicable to the modelled time.');
REPLACE INTO data_quality_time VALUES (2, 'Good - From a different but similar time or only slightly out of date. Still highly relevant.');
REPLACE INTO data_quality_time VALUES (3, 'Acceptable - From a somewhat similar time or several years out of date but still relevant.');
REPLACE INTO data_quality_time VALUES (4, 'Lacking - From a time with different conditions or significantly out of date. Questionable relevance.');
REPLACE INTO data_quality_time VALUES (5, 'Unacceptable - From an irrelevant time or badly out of date.');

CREATE TABLE IF NOT EXISTS data_source_label
(
    source_id TEXT PRIMARY KEY,
    notes     TEXT
);

CREATE TABLE IF NOT EXISTS data_set
(
    data_id     TEXT PRIMARY KEY,
    label       TEXT,
    version     TEXT,
    description TEXT,
    status      TEXT,
    author      TEXT,
    date        TEXT,
    parent_id   TEXT
        REFERENCES data_set (data_id),
    changelog   TEXT,
    notes       TEXT
);

CREATE TABLE IF NOT EXISTS data_source
(
    source_id TEXT,
    source    TEXT,
    notes     TEXT,
    data_id   TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (source_id) REFERENCES data_source_label (source_id),
    PRIMARY KEY (source_id, data_id)
);

-- ============================================================
-- Time tables
-- ============================================================
CREATE TABLE IF NOT EXISTS time_period
(
    sequence INTEGER UNIQUE,
    period   INTEGER PRIMARY KEY,
    flag     TEXT
        REFERENCES time_period_type (label)
);

CREATE TABLE IF NOT EXISTS time_of_day
(
    sequence INTEGER UNIQUE,
    tod      TEXT PRIMARY KEY,
    hours    REAL NOT NULL DEFAULT 1,
    notes    TEXT,
    CHECK (hours > 0)
);

CREATE TABLE IF NOT EXISTS time_season
(
    sequence         INTEGER UNIQUE,
    season           TEXT PRIMARY KEY,
    segment_fraction REAL NOT NULL,
    notes            TEXT,
    CHECK (segment_fraction >= 0 AND segment_fraction <= 1)
);

CREATE TABLE IF NOT EXISTS time_season_sequential
(
    sequence         INTEGER UNIQUE,
    seas_seq         TEXT PRIMARY KEY,
    season           TEXT
        REFERENCES time_season (season),
    segment_fraction REAL NOT NULL,
    notes            TEXT,
    CHECK (segment_fraction >= 0 AND segment_fraction <= 1)
);

-- ============================================================
-- Region
-- ============================================================
CREATE TABLE IF NOT EXISTS region
(
    region TEXT PRIMARY KEY,
    notes  TEXT
);

-- ============================================================
-- Core model definition tables
-- ============================================================
CREATE TABLE IF NOT EXISTS commodity
(
    name        TEXT,
    flag        TEXT
        REFERENCES commodity_type (label),
    description TEXT,
    units       TEXT,
    data_id     TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (name) REFERENCES commodity_label (commodity),
    PRIMARY KEY (name, data_id)
);

CREATE TABLE IF NOT EXISTS technology
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
    data_id      TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (flag) REFERENCES technology_type (label),
    FOREIGN KEY (tech) REFERENCES technology_label (tech),
    PRIMARY KEY (tech, data_id)
);

CREATE TABLE IF NOT EXISTS tech_group
(
    group_name TEXT,
    notes      TEXT,
    data_id    TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (group_name) REFERENCES tech_group_label (group_name),
    PRIMARY KEY (group_name, data_id)
);

CREATE TABLE IF NOT EXISTS tech_group_member
(
    group_name TEXT,
    tech       TEXT,
    data_id    TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (tech) REFERENCES technology_label (tech),
    FOREIGN KEY (group_name) REFERENCES tech_group_label (group_name),
    PRIMARY KEY (group_name, tech, data_id)
);

-- ============================================================
-- Data tables
-- All include: data_id (in PK), data_source, dq_cred/geog/struc/tech/time
-- ============================================================
CREATE TABLE IF NOT EXISTS capacity_credit
(
    region      TEXT,
    period      INTEGER
        REFERENCES time_period (period),
    tech        TEXT,
    vintage     INTEGER,
    credit      REAL,
    notes       TEXT,
    data_source TEXT,
    dq_cred     INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog     INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc    INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech     INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time     INTEGER REFERENCES data_quality_time (dq_time),
    data_id     TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    FOREIGN KEY (tech) REFERENCES technology_label (tech),
    PRIMARY KEY (region, period, tech, vintage, data_id),
    CHECK (credit >= 0 AND credit <= 1)
);

CREATE TABLE IF NOT EXISTS capacity_factor_process
(
    region      TEXT,
    season      TEXT
        REFERENCES time_season (season),
    tod         TEXT
        REFERENCES time_of_day (tod),
    tech        TEXT,
    vintage     INTEGER,
    factor      REAL,
    notes       TEXT,
    data_source TEXT,
    dq_cred     INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog     INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc    INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech     INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time     INTEGER REFERENCES data_quality_time (dq_time),
    data_id     TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    FOREIGN KEY (tech) REFERENCES technology_label (tech),
    PRIMARY KEY (region, season, tod, tech, vintage, data_id),
    CHECK (factor >= 0 AND factor <= 1)
);

CREATE TABLE IF NOT EXISTS capacity_factor_tech
(
    region      TEXT,
    season      TEXT
        REFERENCES time_season (season),
    tod         TEXT
        REFERENCES time_of_day (tod),
    tech        TEXT,
    factor      REAL,
    notes       TEXT,
    data_source TEXT,
    dq_cred     INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog     INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc    INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech     INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time     INTEGER REFERENCES data_quality_time (dq_time),
    data_id     TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    FOREIGN KEY (tech) REFERENCES technology_label (tech),
    PRIMARY KEY (region, season, tod, tech, data_id),
    CHECK (factor >= 0 AND factor <= 1)
);

CREATE TABLE IF NOT EXISTS capacity_to_activity
(
    region      TEXT,
    tech        TEXT,
    c2a         REAL,
    units       TEXT,
    notes       TEXT,
    data_source TEXT,
    dq_cred     INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog     INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc    INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech     INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time     INTEGER REFERENCES data_quality_time (dq_time),
    data_id     TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    FOREIGN KEY (tech) REFERENCES technology_label (tech),
    PRIMARY KEY (region, tech, data_id)
);

CREATE TABLE IF NOT EXISTS construction_input
(
    region      TEXT,
    input_comm  TEXT,
    tech        TEXT,
    vintage     INTEGER
        REFERENCES time_period (period),
    value       REAL,
    units       TEXT,
    notes       TEXT,
    data_source TEXT,
    dq_cred     INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog     INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc    INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech     INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time     INTEGER REFERENCES data_quality_time (dq_time),
    data_id     TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    FOREIGN KEY (tech) REFERENCES technology_label (tech),
    FOREIGN KEY (input_comm) REFERENCES commodity_label (commodity),
    PRIMARY KEY (region, input_comm, tech, vintage, data_id)
);

CREATE TABLE IF NOT EXISTS cost_emission
(
    region      TEXT,
    period      INTEGER
        REFERENCES time_period (period),
    emis_comm   TEXT NOT NULL,
    cost        REAL NOT NULL,
    units       TEXT,
    notes       TEXT,
    data_source TEXT,
    dq_cred     INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog     INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc    INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech     INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time     INTEGER REFERENCES data_quality_time (dq_time),
    data_id     TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    FOREIGN KEY (emis_comm) REFERENCES commodity_label (commodity),
    PRIMARY KEY (region, period, emis_comm, data_id)
);

CREATE TABLE IF NOT EXISTS cost_fixed
(
    region      TEXT    NOT NULL,
    period      INTEGER NOT NULL
        REFERENCES time_period (period),
    tech        TEXT    NOT NULL,
    vintage     INTEGER NOT NULL
        REFERENCES time_period (period),
    cost        REAL,
    units       TEXT,
    notes       TEXT,
    data_source TEXT,
    dq_cred     INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog     INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc    INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech     INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time     INTEGER REFERENCES data_quality_time (dq_time),
    data_id     TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    FOREIGN KEY (tech) REFERENCES technology_label (tech),
    PRIMARY KEY (region, period, tech, vintage, data_id)
);

CREATE TABLE IF NOT EXISTS cost_invest
(
    region      TEXT,
    tech        TEXT,
    vintage     INTEGER
        REFERENCES time_period (period),
    cost        REAL,
    units       TEXT,
    notes       TEXT,
    data_source TEXT,
    dq_cred     INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog     INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc    INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech     INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time     INTEGER REFERENCES data_quality_time (dq_time),
    data_id     TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    FOREIGN KEY (tech) REFERENCES technology_label (tech),
    PRIMARY KEY (region, tech, vintage, data_id)
);

CREATE TABLE IF NOT EXISTS cost_variable
(
    region      TEXT    NOT NULL,
    period      INTEGER NOT NULL
        REFERENCES time_period (period),
    tech        TEXT    NOT NULL,
    vintage     INTEGER NOT NULL
        REFERENCES time_period (period),
    cost        REAL,
    units       TEXT,
    notes       TEXT,
    data_source TEXT,
    dq_cred     INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog     INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc    INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech     INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time     INTEGER REFERENCES data_quality_time (dq_time),
    data_id     TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    FOREIGN KEY (tech) REFERENCES technology_label (tech),
    PRIMARY KEY (region, period, tech, vintage, data_id)
);

CREATE TABLE IF NOT EXISTS demand
(
    region      TEXT,
    period      INTEGER
        REFERENCES time_period (period),
    commodity   TEXT,
    demand      REAL,
    units       TEXT,
    notes       TEXT,
    data_source TEXT,
    dq_cred     INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog     INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc    INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech     INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time     INTEGER REFERENCES data_quality_time (dq_time),
    data_id     TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    FOREIGN KEY (commodity) REFERENCES commodity_label (commodity),
    PRIMARY KEY (region, period, commodity, data_id)
);

CREATE TABLE IF NOT EXISTS demand_specific_distribution
(
    region      TEXT,
    period      INTEGER
        REFERENCES time_period (period),
    season      TEXT
        REFERENCES time_season (season),
    tod         TEXT
        REFERENCES time_of_day (tod),
    demand_name TEXT,
    dsd         REAL,
    notes       TEXT,
    data_source TEXT,
    dq_cred     INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog     INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc    INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech     INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time     INTEGER REFERENCES data_quality_time (dq_time),
    data_id     TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    FOREIGN KEY (demand_name) REFERENCES commodity_label (commodity),
    PRIMARY KEY (region, period, season, tod, demand_name, data_id),
    CHECK (dsd >= 0 AND dsd <= 1)
);

CREATE TABLE IF NOT EXISTS end_of_life_output
(
    region      TEXT,
    tech        TEXT,
    vintage     INTEGER
        REFERENCES time_period (period),
    output_comm TEXT,
    value       REAL,
    units       TEXT,
    notes       TEXT,
    data_source TEXT,
    dq_cred     INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog     INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc    INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech     INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time     INTEGER REFERENCES data_quality_time (dq_time),
    data_id     TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    FOREIGN KEY (tech) REFERENCES technology_label (tech),
    FOREIGN KEY (output_comm) REFERENCES commodity_label (commodity),
    PRIMARY KEY (region, tech, vintage, output_comm, data_id)
);

CREATE TABLE IF NOT EXISTS efficiency
(
    region      TEXT,
    input_comm  TEXT,
    tech        TEXT,
    vintage     INTEGER
        REFERENCES time_period (period),
    output_comm TEXT,
    efficiency  REAL,
    units       TEXT,
    notes       TEXT,
    data_source TEXT,
    dq_cred     INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog     INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc    INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech     INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time     INTEGER REFERENCES data_quality_time (dq_time),
    data_id     TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    FOREIGN KEY (tech) REFERENCES technology_label (tech),
    FOREIGN KEY (input_comm) REFERENCES commodity_label (commodity),
    FOREIGN KEY (output_comm) REFERENCES commodity_label (commodity),
    PRIMARY KEY (region, input_comm, tech, vintage, output_comm, data_id),
    CHECK (efficiency > 0)
);

CREATE TABLE IF NOT EXISTS efficiency_variable
(
    region      TEXT,
    season      TEXT
        REFERENCES time_season (season),
    tod         TEXT
        REFERENCES time_of_day (tod),
    input_comm  TEXT,
    tech        TEXT,
    vintage     INTEGER
        REFERENCES time_period (period),
    output_comm TEXT,
    efficiency  REAL,
    notes       TEXT,
    data_source TEXT,
    dq_cred     INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog     INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc    INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech     INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time     INTEGER REFERENCES data_quality_time (dq_time),
    data_id     TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    FOREIGN KEY (tech) REFERENCES technology_label (tech),
    FOREIGN KEY (input_comm) REFERENCES commodity_label (commodity),
    FOREIGN KEY (output_comm) REFERENCES commodity_label (commodity),
    PRIMARY KEY (region, season, tod, input_comm, tech, vintage, output_comm, data_id),
    CHECK (efficiency > 0)
);

CREATE TABLE IF NOT EXISTS emission_activity
(
    region      TEXT,
    emis_comm   TEXT,
    input_comm  TEXT,
    tech        TEXT,
    vintage     INTEGER
        REFERENCES time_period (period),
    output_comm TEXT,
    activity    REAL,
    units       TEXT,
    notes       TEXT,
    data_source TEXT,
    dq_cred     INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog     INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc    INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech     INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time     INTEGER REFERENCES data_quality_time (dq_time),
    data_id     TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    FOREIGN KEY (tech) REFERENCES technology_label (tech),
    FOREIGN KEY (emis_comm) REFERENCES commodity_label (commodity),
    FOREIGN KEY (input_comm) REFERENCES commodity_label (commodity),
    FOREIGN KEY (output_comm) REFERENCES commodity_label (commodity),
    PRIMARY KEY (region, emis_comm, input_comm, tech, vintage, output_comm, data_id)
);

CREATE TABLE IF NOT EXISTS emission_embodied
(
    region      TEXT,
    emis_comm   TEXT,
    tech        TEXT,
    vintage     INTEGER
        REFERENCES time_period (period),
    value       REAL,
    units       TEXT,
    notes       TEXT,
    data_source TEXT,
    dq_cred     INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog     INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc    INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech     INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time     INTEGER REFERENCES data_quality_time (dq_time),
    data_id     TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    FOREIGN KEY (tech) REFERENCES technology_label (tech),
    FOREIGN KEY (emis_comm) REFERENCES commodity_label (commodity),
    PRIMARY KEY (region, emis_comm, tech, vintage, data_id)
);

CREATE TABLE IF NOT EXISTS emission_end_of_life
(
    region      TEXT,
    emis_comm   TEXT,
    tech        TEXT,
    vintage     INTEGER
        REFERENCES time_period (period),
    value       REAL,
    units       TEXT,
    notes       TEXT,
    data_source TEXT,
    dq_cred     INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog     INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc    INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech     INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time     INTEGER REFERENCES data_quality_time (dq_time),
    data_id     TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    FOREIGN KEY (tech) REFERENCES technology_label (tech),
    FOREIGN KEY (emis_comm) REFERENCES commodity_label (commodity),
    PRIMARY KEY (region, emis_comm, tech, vintage, data_id)
);

CREATE TABLE IF NOT EXISTS existing_capacity
(
    region      TEXT,
    tech        TEXT,
    vintage     INTEGER
        REFERENCES time_period (period),
    capacity    REAL,
    units       TEXT,
    notes       TEXT,
    data_source TEXT,
    dq_cred     INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog     INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc    INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech     INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time     INTEGER REFERENCES data_quality_time (dq_time),
    data_id     TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    FOREIGN KEY (tech) REFERENCES technology_label (tech),
    PRIMARY KEY (region, tech, vintage, data_id)
);

CREATE TABLE IF NOT EXISTS loan_lifetime_process
(
    region      TEXT,
    tech        TEXT,
    vintage     INTEGER
        REFERENCES time_period (period),
    lifetime    REAL,
    units       TEXT,
    notes       TEXT,
    data_source TEXT,
    dq_cred     INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog     INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc    INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech     INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time     INTEGER REFERENCES data_quality_time (dq_time),
    data_id     TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    FOREIGN KEY (tech) REFERENCES technology_label (tech),
    PRIMARY KEY (region, tech, vintage, data_id)
);

CREATE TABLE IF NOT EXISTS loan_rate
(
    region      TEXT,
    tech        TEXT,
    vintage     INTEGER
        REFERENCES time_period (period),
    rate        REAL,
    notes       TEXT,
    data_source TEXT,
    dq_cred     INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog     INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc    INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech     INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time     INTEGER REFERENCES data_quality_time (dq_time),
    data_id     TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    FOREIGN KEY (tech) REFERENCES technology_label (tech),
    PRIMARY KEY (region, tech, vintage, data_id)
);

CREATE TABLE IF NOT EXISTS lifetime_process
(
    region      TEXT,
    tech        TEXT,
    vintage     INTEGER
        REFERENCES time_period (period),
    lifetime    REAL,
    units       TEXT,
    notes       TEXT,
    data_source TEXT,
    dq_cred     INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog     INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc    INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech     INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time     INTEGER REFERENCES data_quality_time (dq_time),
    data_id     TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    FOREIGN KEY (tech) REFERENCES technology_label (tech),
    PRIMARY KEY (region, tech, vintage, data_id)
);

CREATE TABLE IF NOT EXISTS lifetime_tech
(
    region      TEXT,
    tech        TEXT,
    lifetime    REAL,
    units       TEXT,
    notes       TEXT,
    data_source TEXT,
    dq_cred     INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog     INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc    INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech     INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time     INTEGER REFERENCES data_quality_time (dq_time),
    data_id     TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    FOREIGN KEY (tech) REFERENCES technology_label (tech),
    PRIMARY KEY (region, tech, data_id)
);

CREATE TABLE IF NOT EXISTS limit_growth_capacity
(
    region        TEXT,
    tech_or_group TEXT,
    operator      TEXT NOT NULL DEFAULT "le"
        REFERENCES operator (operator),
    rate          REAL NOT NULL DEFAULT 0,
    seed          REAL NOT NULL DEFAULT 0,
    seed_units    TEXT,
    notes         TEXT,
    data_source   TEXT,
    dq_cred       INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog       INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc      INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech       INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time       INTEGER REFERENCES data_quality_time (dq_time),
    data_id       TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    PRIMARY KEY (region, tech_or_group, operator, data_id)
);

CREATE TABLE IF NOT EXISTS limit_degrowth_capacity
(
    region        TEXT,
    tech_or_group TEXT,
    operator      TEXT NOT NULL DEFAULT "le"
        REFERENCES operator (operator),
    rate          REAL NOT NULL DEFAULT 0,
    seed          REAL NOT NULL DEFAULT 0,
    seed_units    TEXT,
    notes         TEXT,
    data_source   TEXT,
    dq_cred       INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog       INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc      INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech       INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time       INTEGER REFERENCES data_quality_time (dq_time),
    data_id       TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    PRIMARY KEY (region, tech_or_group, operator, data_id)
);

CREATE TABLE IF NOT EXISTS limit_growth_new_capacity
(
    region        TEXT,
    tech_or_group TEXT,
    operator      TEXT NOT NULL DEFAULT "le"
        REFERENCES operator (operator),
    rate          REAL NOT NULL DEFAULT 0,
    seed          REAL NOT NULL DEFAULT 0,
    seed_units    TEXT,
    notes         TEXT,
    data_source   TEXT,
    dq_cred       INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog       INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc      INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech       INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time       INTEGER REFERENCES data_quality_time (dq_time),
    data_id       TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    PRIMARY KEY (region, tech_or_group, operator, data_id)
);

CREATE TABLE IF NOT EXISTS limit_degrowth_new_capacity
(
    region        TEXT,
    tech_or_group TEXT,
    operator      TEXT NOT NULL DEFAULT "le"
        REFERENCES operator (operator),
    rate          REAL NOT NULL DEFAULT 0,
    seed          REAL NOT NULL DEFAULT 0,
    seed_units    TEXT,
    notes         TEXT,
    data_source   TEXT,
    dq_cred       INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog       INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc      INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech       INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time       INTEGER REFERENCES data_quality_time (dq_time),
    data_id       TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    PRIMARY KEY (region, tech_or_group, operator, data_id)
);

CREATE TABLE IF NOT EXISTS limit_growth_new_capacity_delta
(
    region        TEXT,
    tech_or_group TEXT,
    operator      TEXT NOT NULL DEFAULT "le"
        REFERENCES operator (operator),
    rate          REAL NOT NULL DEFAULT 0,
    seed          REAL NOT NULL DEFAULT 0,
    seed_units    TEXT,
    notes         TEXT,
    data_source   TEXT,
    dq_cred       INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog       INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc      INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech       INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time       INTEGER REFERENCES data_quality_time (dq_time),
    data_id       TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    PRIMARY KEY (region, tech_or_group, operator, data_id)
);

CREATE TABLE IF NOT EXISTS limit_degrowth_new_capacity_delta
(
    region        TEXT,
    tech_or_group TEXT,
    operator      TEXT NOT NULL DEFAULT "le"
        REFERENCES operator (operator),
    rate          REAL NOT NULL DEFAULT 0,
    seed          REAL NOT NULL DEFAULT 0,
    seed_units    TEXT,
    notes         TEXT,
    data_source   TEXT,
    dq_cred       INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog       INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc      INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech       INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time       INTEGER REFERENCES data_quality_time (dq_time),
    data_id       TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    PRIMARY KEY (region, tech_or_group, operator, data_id)
);

CREATE TABLE IF NOT EXISTS limit_storage_level_fraction
(
    region      TEXT,
    season      TEXT
        REFERENCES time_season (season),
    tod         TEXT
        REFERENCES time_of_day (tod),
    tech        TEXT,
    operator    TEXT NOT NULL DEFAULT "le"
        REFERENCES operator (operator),
    fraction    REAL,
    notes       TEXT,
    data_source TEXT,
    dq_cred     INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog     INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc    INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech     INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time     INTEGER REFERENCES data_quality_time (dq_time),
    data_id     TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    FOREIGN KEY (tech) REFERENCES technology_label (tech),
    CHECK (fraction >= 0 AND fraction <= 1),
    PRIMARY KEY (region, season, tod, tech, operator, data_id)
);

CREATE TABLE IF NOT EXISTS limit_activity
(
    region        TEXT,
    period        INTEGER
        REFERENCES time_period (period),
    tech_or_group TEXT,
    operator      TEXT NOT NULL DEFAULT "le"
        REFERENCES operator (operator),
    activity      REAL,
    units         TEXT,
    notes         TEXT,
    data_source   TEXT,
    dq_cred       INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog       INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc      INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech       INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time       INTEGER REFERENCES data_quality_time (dq_time),
    data_id       TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    PRIMARY KEY (region, period, tech_or_group, operator, data_id)
);

CREATE TABLE IF NOT EXISTS limit_activity_share
(
    region      TEXT,
    period      INTEGER
        REFERENCES time_period (period),
    sub_group   TEXT,
    super_group TEXT,
    operator    TEXT NOT NULL DEFAULT "le"
        REFERENCES operator (operator),
    share       REAL,
    notes       TEXT,
    data_source TEXT,
    dq_cred     INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog     INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc    INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech     INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time     INTEGER REFERENCES data_quality_time (dq_time),
    data_id     TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    PRIMARY KEY (region, period, sub_group, super_group, operator, data_id)
);

CREATE TABLE IF NOT EXISTS limit_annual_capacity_factor
(
    region        TEXT,
    tech_or_group TEXT,
    vintage       INTEGER
        REFERENCES time_period (period),
    output_comm   TEXT,
    operator      TEXT NOT NULL DEFAULT "le"
        REFERENCES operator (operator),
    factor        REAL,
    notes         TEXT,
    data_source   TEXT,
    dq_cred       INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog       INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc      INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech       INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time       INTEGER REFERENCES data_quality_time (dq_time),
    data_id       TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    FOREIGN KEY (output_comm) REFERENCES commodity_label (commodity),
    PRIMARY KEY (region, tech_or_group, vintage, output_comm, operator, data_id),
    CHECK (factor >= 0 AND factor <= 1)
);

CREATE TABLE IF NOT EXISTS limit_capacity
(
    region        TEXT,
    period        INTEGER
        REFERENCES time_period (period),
    tech_or_group TEXT,
    operator      TEXT NOT NULL DEFAULT "le"
        REFERENCES operator (operator),
    capacity      REAL,
    units         TEXT,
    notes         TEXT,
    data_source   TEXT,
    dq_cred       INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog       INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc      INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech       INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time       INTEGER REFERENCES data_quality_time (dq_time),
    data_id       TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    PRIMARY KEY (region, period, tech_or_group, operator, data_id)
);

CREATE TABLE IF NOT EXISTS limit_capacity_share
(
    region      TEXT,
    period      INTEGER
        REFERENCES time_period (period),
    sub_group   TEXT,
    super_group TEXT,
    operator    TEXT NOT NULL DEFAULT "le"
        REFERENCES operator (operator),
    share       REAL,
    notes       TEXT,
    data_source TEXT,
    dq_cred     INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog     INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc    INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech     INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time     INTEGER REFERENCES data_quality_time (dq_time),
    data_id     TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    PRIMARY KEY (region, period, sub_group, super_group, operator, data_id)
);

CREATE TABLE IF NOT EXISTS limit_new_capacity
(
    region        TEXT,
    tech_or_group TEXT,
    vintage       INTEGER
        REFERENCES time_period (period),
    operator      TEXT NOT NULL DEFAULT "le"
        REFERENCES operator (operator),
    new_cap       REAL,
    units         TEXT,
    notes         TEXT,
    data_source   TEXT,
    dq_cred       INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog       INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc      INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech       INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time       INTEGER REFERENCES data_quality_time (dq_time),
    data_id       TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    PRIMARY KEY (region, tech_or_group, vintage, operator, data_id)
);

CREATE TABLE IF NOT EXISTS limit_new_capacity_share
(
    region      TEXT,
    sub_group   TEXT,
    super_group TEXT,
    vintage     INTEGER
        REFERENCES time_period (period),
    operator    TEXT NOT NULL DEFAULT "le"
        REFERENCES operator (operator),
    share       REAL,
    notes       TEXT,
    data_source TEXT,
    dq_cred     INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog     INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc    INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech     INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time     INTEGER REFERENCES data_quality_time (dq_time),
    data_id     TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    PRIMARY KEY (region, sub_group, super_group, vintage, operator, data_id)
);

CREATE TABLE IF NOT EXISTS limit_resource
(
    region        TEXT,
    tech_or_group TEXT,
    operator      TEXT NOT NULL DEFAULT "le"
        REFERENCES operator (operator),
    cum_act       REAL,
    units         TEXT,
    notes         TEXT,
    data_source   TEXT,
    dq_cred       INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog       INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc      INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech       INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time       INTEGER REFERENCES data_quality_time (dq_time),
    data_id       TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    PRIMARY KEY (region, tech_or_group, operator, data_id)
);

CREATE TABLE IF NOT EXISTS limit_seasonal_capacity_factor
(
    region        TEXT
        REFERENCES region (region),
    season        TEXT
        REFERENCES time_season (season),
    tech_or_group TEXT,
    operator      TEXT NOT NULL DEFAULT "le"
        REFERENCES operator (operator),
    factor        REAL,
    notes         TEXT,
    data_source   TEXT,
    dq_cred       INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog       INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc      INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech       INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time       INTEGER REFERENCES data_quality_time (dq_time),
    data_id       TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    PRIMARY KEY (region, season, tech_or_group, operator, data_id)
);

CREATE TABLE IF NOT EXISTS limit_tech_input_split
(
    region      TEXT,
    period      INTEGER
        REFERENCES time_period (period),
    input_comm  TEXT,
    tech        TEXT,
    operator    TEXT NOT NULL DEFAULT "le"
        REFERENCES operator (operator),
    proportion  REAL,
    notes       TEXT,
    data_source TEXT,
    dq_cred     INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog     INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc    INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech     INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time     INTEGER REFERENCES data_quality_time (dq_time),
    data_id     TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    FOREIGN KEY (tech) REFERENCES technology_label (tech),
    FOREIGN KEY (input_comm) REFERENCES commodity_label (commodity),
    PRIMARY KEY (region, period, input_comm, tech, operator, data_id)
);

CREATE TABLE IF NOT EXISTS limit_tech_input_split_annual
(
    region      TEXT,
    period      INTEGER
        REFERENCES time_period (period),
    input_comm  TEXT,
    tech        TEXT,
    operator    TEXT NOT NULL DEFAULT "le"
        REFERENCES operator (operator),
    proportion  REAL,
    notes       TEXT,
    data_source TEXT,
    dq_cred     INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog     INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc    INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech     INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time     INTEGER REFERENCES data_quality_time (dq_time),
    data_id     TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    FOREIGN KEY (tech) REFERENCES technology_label (tech),
    PRIMARY KEY (region, period, input_comm, tech, operator, data_id)
);

CREATE TABLE IF NOT EXISTS limit_tech_output_split
(
    region      TEXT,
    period      INTEGER
        REFERENCES time_period (period),
    tech        TEXT,
    output_comm TEXT,
    operator    TEXT NOT NULL DEFAULT "le"
        REFERENCES operator (operator),
    proportion  REAL,
    notes       TEXT,
    data_source TEXT,
    dq_cred     INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog     INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc    INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech     INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time     INTEGER REFERENCES data_quality_time (dq_time),
    data_id     TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    FOREIGN KEY (tech) REFERENCES technology_label (tech),
    FOREIGN KEY (output_comm) REFERENCES commodity_label (commodity),
    PRIMARY KEY (region, period, tech, output_comm, operator, data_id)
);

CREATE TABLE IF NOT EXISTS limit_tech_output_split_annual
(
    region      TEXT,
    period      INTEGER
        REFERENCES time_period (period),
    tech        TEXT,
    output_comm TEXT,
    operator    TEXT NOT NULL DEFAULT "le"
        REFERENCES operator (operator),
    proportion  REAL,
    notes       TEXT,
    data_source TEXT,
    dq_cred     INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog     INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc    INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech     INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time     INTEGER REFERENCES data_quality_time (dq_time),
    data_id     TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    FOREIGN KEY (tech) REFERENCES technology_label (tech),
    FOREIGN KEY (output_comm) REFERENCES commodity_label (commodity),
    PRIMARY KEY (region, period, tech, output_comm, operator, data_id)
);

CREATE TABLE IF NOT EXISTS limit_emission
(
    region      TEXT,
    period      INTEGER
        REFERENCES time_period (period),
    emis_comm   TEXT,
    operator    TEXT NOT NULL DEFAULT "le"
        REFERENCES operator (operator),
    value       REAL,
    units       TEXT,
    notes       TEXT,
    data_source TEXT,
    dq_cred     INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog     INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc    INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech     INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time     INTEGER REFERENCES data_quality_time (dq_time),
    data_id     TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    FOREIGN KEY (emis_comm) REFERENCES commodity_label (commodity),
    PRIMARY KEY (region, period, emis_comm, operator, data_id)
);

CREATE TABLE IF NOT EXISTS linked_tech
(
    primary_region TEXT,
    primary_tech   TEXT,
    emis_comm      TEXT,
    driven_tech    TEXT,
    notes          TEXT,
    data_source TEXT,
    data_id        TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (primary_tech) REFERENCES technology_label (tech),
    FOREIGN KEY (driven_tech) REFERENCES technology_label (tech),
    FOREIGN KEY (emis_comm) REFERENCES commodity_label (commodity),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    PRIMARY KEY (primary_region, primary_tech, emis_comm, data_id)
);

CREATE TABLE IF NOT EXISTS planning_reserve_margin
(
    region      TEXT,
    margin      REAL,
    notes       TEXT,
    data_source TEXT,
    dq_cred     INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog     INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc    INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech     INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time     INTEGER REFERENCES data_quality_time (dq_time),
    data_id     TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    FOREIGN KEY (region) REFERENCES region (region),
    PRIMARY KEY (region, data_id)
);

CREATE TABLE IF NOT EXISTS ramp_down_hourly
(
    region      TEXT,
    tech        TEXT,
    rate        REAL,
    notes       TEXT,
    data_source TEXT,
    dq_cred     INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog     INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc    INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech     INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time     INTEGER REFERENCES data_quality_time (dq_time),
    data_id     TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    FOREIGN KEY (tech) REFERENCES technology_label (tech),
    PRIMARY KEY (region, tech, data_id)
);

CREATE TABLE IF NOT EXISTS ramp_up_hourly
(
    region      TEXT,
    tech        TEXT,
    rate        REAL,
    notes       TEXT,
    data_source TEXT,
    dq_cred     INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog     INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc    INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech     INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time     INTEGER REFERENCES data_quality_time (dq_time),
    data_id     TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    FOREIGN KEY (tech) REFERENCES technology_label (tech),
    PRIMARY KEY (region, tech, data_id)
);

CREATE TABLE IF NOT EXISTS reserve_capacity_derate
(
    region      TEXT,
    season      TEXT
        REFERENCES time_season (season),
    tech        TEXT,
    vintage     INTEGER,
    factor      REAL,
    notes       TEXT,
    data_source TEXT,
    dq_cred     INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog     INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc    INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech     INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time     INTEGER REFERENCES data_quality_time (dq_time),
    data_id     TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    FOREIGN KEY (tech) REFERENCES technology_label (tech),
    PRIMARY KEY (region, season, tech, vintage, data_id),
    CHECK (factor >= 0 AND factor <= 1)
);

CREATE TABLE IF NOT EXISTS storage_duration
(
    region      TEXT,
    tech        TEXT,
    duration    REAL,
    notes       TEXT,
    data_source TEXT,
    dq_cred     INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog     INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc    INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech     INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time     INTEGER REFERENCES data_quality_time (dq_time),
    data_id     TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    PRIMARY KEY (region, tech, data_id)
);

CREATE TABLE IF NOT EXISTS lifetime_survival_curve
(
    region      TEXT    NOT NULL,
    period      INTEGER NOT NULL,
    tech        TEXT    NOT NULL,
    vintage     INTEGER NOT NULL
        REFERENCES time_period (period),
    fraction    REAL,
    notes       TEXT,
    data_source TEXT,
    dq_cred     INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog     INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc    INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech     INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time     INTEGER REFERENCES data_quality_time (dq_time),
    data_id     TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    FOREIGN KEY (tech) REFERENCES technology_label (tech),
    PRIMARY KEY (region, period, tech, vintage, data_id)
);

CREATE TABLE IF NOT EXISTS rps_requirement
(
    region      TEXT    NOT NULL
        REFERENCES region (region),
    period      INTEGER NOT NULL
        REFERENCES time_period (period),
    tech_group  TEXT    NOT NULL,
    requirement REAL    NOT NULL,
    notes       TEXT,
    data_source TEXT,
    dq_cred     INTEGER REFERENCES data_quality_credibility (dq_cred),
    dq_geog     INTEGER REFERENCES data_quality_geography (dq_geog),
    dq_struc    INTEGER REFERENCES data_quality_structure (dq_struc),
    dq_tech     INTEGER REFERENCES data_quality_technology (dq_tech),
    dq_time     INTEGER REFERENCES data_quality_time (dq_time),
    data_id     TEXT
        REFERENCES data_set (data_id),
    FOREIGN KEY (data_source) REFERENCES data_source_label (source_id),
    FOREIGN KEY (tech_group) REFERENCES tech_group_label (group_name),
    PRIMARY KEY (region, period, tech_group, data_id)
);

COMMIT;
PRAGMA foreign_keys = ON;
