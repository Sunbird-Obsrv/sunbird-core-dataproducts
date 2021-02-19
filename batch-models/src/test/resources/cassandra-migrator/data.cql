CREATE KEYSPACE IF NOT EXISTS test_keyspace WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '1'
};

CREATE TABLE IF NOT EXISTS test_keyspace.user_enrolment_test(
    batchid text,
    active boolean,
    addedby text,
    certificates list<frozen<map<text, text>>>,
    certstatus int,
    completedon timestamp,
    completionpercentage int,
    contentstatus map<text, int>,
    courseid text,
    datetime timestamp,
    enrolleddate text,
    issued_certificates list<frozen<map<text, text>>>,
    lastreadcontentid text,
    lastreadcontentstatus int,
    progress int,
    status int,
    userid text,
    PRIMARY KEY(userid, courseid, batchid))
    WITH COMPACTION = { 'class': 'SizeTieredCompactionStrategy', 'enabled': 'false' };


INSERT INTO test_keyspace.user_enrolment_test(batchid, userid,courseid) VALUES ('b1','u1','c1');