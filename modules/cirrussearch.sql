-- Tables for the CirrusSearch dumps module

CREATE TABLE cirrussearch (
    dumpdate DATE,
    claimed_by VARCHAR(255),
    can_archive INT,
    is_archived INT,
    is_checked INT,
    comments VARCHAR(255)
);

CREATE INDEX is_archived ON cirrussearch (is_archived);
CREATE INDEX is_checked ON cirrussearch (is_checked);
