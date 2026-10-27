-- Separate database for the test suite, so tests never touch the service's data.
CREATE DATABASE payments_test OWNER payments;
