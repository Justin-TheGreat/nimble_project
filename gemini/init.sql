-- init.sql

-- Create the robot table
CREATE TABLE robot (
    UUID VARCHAR(36) PRIMARY KEY,
    Robot_ID VARCHAR(50) NOT NULL,
    Timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    Event_ID VARCHAR(50)
);

-- Create the teleop table
CREATE TABLE teleop (
    UUID VARCHAR(36) PRIMARY KEY,
    Robot_ID VARCHAR(50) NOT NULL,
    Timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    Event_ID VARCHAR(50),
    Teleoperator_ID VARCHAR(50)
);

-- Create a function to send notifications for the robot table
CREATE OR REPLACE FUNCTION notify_robot_change()
RETURNS TRIGGER AS $$
DECLARE
  payload TEXT;
BEGIN
  -- Construct JSON payload. NEW refers to the row being inserted/updated.
  payload := json_build_object(
    'table', TG_TABLE_NAME,
    'action', TG_OP, -- INSERT, UPDATE, DELETE
    'data', row_to_json(NEW)
  )::TEXT;

  -- Send notification on the 'db_changes' channel with the payload
  -- PERFORM pg_notify('db_changes', payload);
  -- Alternative: Send specific channel per table
  PERFORM pg_notify('robot_changes', payload);

  RETURN NEW; -- For INSERT/UPDATE triggers
END;
$$ LANGUAGE plpgsql;

-- Create a trigger on the robot table for INSERT operations
CREATE TRIGGER robot_insert_trigger
AFTER INSERT ON robot
FOR EACH ROW EXECUTE FUNCTION notify_robot_change();

-- Create a function to send notifications for the teleop table
CREATE OR REPLACE FUNCTION notify_teleop_change()
RETURNS TRIGGER AS $$
DECLARE
  payload TEXT;
BEGIN
  payload := json_build_object(
    'table', TG_TABLE_NAME,
    'action', TG_OP,
    'data', row_to_json(NEW)
  )::TEXT;

  PERFORM pg_notify('teleop_changes', payload);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create a trigger on the teleop table for INSERT operations
CREATE TRIGGER teleop_insert_trigger
AFTER INSERT ON teleop
FOR EACH ROW EXECUTE FUNCTION notify_teleop_change();

-- Optional: Add indexes if needed for query performance (not directly related to CDC)
-- CREATE INDEX idx_robot_timestamp ON robot(Timestamp);
-- CREATE INDEX idx_teleop_timestamp ON teleop(Timestamp);
-- CREATE INDEX idx_robot_id ON robot(Robot_ID);
-- CREATE INDEX idx_teleop_robot_id ON teleop(Robot_ID);

