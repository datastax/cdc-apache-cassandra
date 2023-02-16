# CDC Backfill Client

A tool that reads rows from an existing Cassandra table and send their primary keys to the event topic. This will
trigger the Cassandra Source Connector (CSC) to populate the date topics with historical rows (i.e. existed 
before enabling cdc on the table). Please note the CSC only guarantees sending the latest state of rows and not
necessarily their intermediary states. 
