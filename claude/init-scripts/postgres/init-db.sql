-- Create database tables
CREATE TABLE robot (
    UUID VARCHAR(36) PRIMARY KEY,
    Robot_ID VARCHAR(50) NOT NULL,
    Timestamp TIMESTAMP NOT NULL,
    Event_ID VARCHAR(50) NOT NULL
);

CREATE TABLE teleop (
    UUID VARCHAR(36) PRIMARY KEY,
    Robot_ID VARCHAR(50) NOT NULL,
    Timestamp TIMESTAMP NOT NULL,
    Event_ID VARCHAR(50) NOT NULL,
    Teleoperator_ID VARCHAR(50) NOT NULL
);

-- Create Kafka publication
CREATE PUBLICATION kafka_publication FOR ALL TABLES;

-- Create functions for CDC (Change Data Capture)
CREATE OR REPLACE FUNCTION notify_robot_changes()
RETURNS TRIGGER AS $$
DECLARE
    payload TEXT;
BEGIN
    IF (TG_OP = 'DELETE') THEN
        payload = json_build_object(
            'operation', TG_OP,
            'table', TG_TABLE_NAME,
            'uuid', OLD.UUID,
            'robot_id', OLD.Robot_ID,
            'timestamp', OLD.Timestamp,
            'event_id', OLD.Event_ID
        );
    ELSE
        payload = json_build_object(
            'operation', TG_OP,
            'table', TG_TABLE_NAME,
            'uuid', NEW.UUID,
            'robot_id', NEW.Robot_ID,
            'timestamp', NEW.Timestamp,
            'event_id', NEW.Event_ID
        );
    END IF;

    PERFORM pg_notify('robot_changes', payload);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION notify_teleop_changes()
RETURNS TRIGGER AS $$
DECLARE
    payload TEXT;
BEGIN
    IF (TG_OP = 'DELETE') THEN
        payload = json_build_object(
            'operation', TG_OP,
            'table', TG_TABLE_NAME,
            'uuid', OLD.UUID,
            'robot_id', OLD.Robot_ID,
            'timestamp', OLD.Timestamp,
            'event_id', OLD.Event_ID,
            'teleoperator_id', OLD.Teleoperator_ID
        );
    ELSE
        payload = json_build_object(
            'operation', TG_OP,
            'table', TG_TABLE_NAME,
            'uuid', NEW.UUID,
            'robot_id', NEW.Robot_ID,
            'timestamp', NEW.Timestamp,
            'event_id', NEW.Event_ID,
            'teleoperator_id', NEW.Teleoperator_ID
        );
    END IF;

    PERFORM pg_notify('teleop_changes', payload);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Create triggers for CDC
CREATE TRIGGER robot_changes_trigger
AFTER INSERT OR UPDATE OR DELETE ON robot
FOR EACH ROW EXECUTE FUNCTION notify_robot_changes();

CREATE TRIGGER teleop_changes_trigger
AFTER INSERT OR UPDATE OR DELETE ON teleop
FOR EACH ROW EXECUTE FUNCTION notify_teleop_changes();

-- Create Logical Replication Slot
SELECT pg_create_logical_replication_slot('kafka_slot', 'pgoutput');