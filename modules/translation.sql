-- Tables for the Content Translation dumps module

CREATE TABLE translation (
    dumpdate DATE,
    claimed_by VARCHAR(255),
    can_archive INT,
    is_archived INT,
    is_checked INT,
    comments VARCHAR(255)
);

CREATE INDEX is_archived ON translation (is_archived);
CREATE INDEX is_checked ON translation (is_checked);
