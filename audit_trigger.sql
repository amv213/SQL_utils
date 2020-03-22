-- Make sure to change console dialect in DataGrip to PostgreSQL

-- Create table to hold dispatched data
CREATE TABLE IF NOT EXISTS data_container_last AS
    SELECT * FROM data_container LIMIT 1;

-- Set trigger channel name for clients to LISTEN on
CREATE TRIGGER table_changed
BEFORE INSERT OR UPDATE
ON data_container
FOR EACH ROW
EXECUTE PROCEDURE notify_update();

-- Define procedure on trigger
CREATE OR REPLACE FUNCTION notify_update()
RETURNS trigger AS $$
BEGIN

    -- update all rows in the table (there is only one) with NEW values
    UPDATE data_container_last
    SET id = NEW.id;


    -- Can add payload string to NOTIFY but here we do payload free for speed
    -- PERFORM pg_notify('table_changed', 'a random payload');
    NOTIFY table_changed;

    -- Return NEW if TRIGGER is BEFORE <INSERT/UPDATE/...> ; Else return NULL.
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;