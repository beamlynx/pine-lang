-- Readonly User Setup for Pine Database
-- This creates a readonly user that can only perform SELECT operations
-- on all tables in the public and audit schemas

-- Create readonly role
CREATE ROLE pine_readonly WITH LOGIN PASSWORD 'pine_readonly';

-- Grant connection privileges
GRANT CONNECT ON DATABASE pine TO pine_readonly;

-- Grant usage on schemas
GRANT USAGE ON SCHEMA public TO pine_readonly;
GRANT USAGE ON SCHEMA audit TO pine_readonly;

-- Grant SELECT privileges on all existing tables in public schema
GRANT SELECT ON ALL TABLES IN SCHEMA public TO pine_readonly;

-- Grant SELECT privileges on all existing tables in audit schema  
GRANT SELECT ON ALL TABLES IN SCHEMA audit TO pine_readonly;

-- Grant SELECT privileges on all future tables in public schema
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO pine_readonly;

-- Grant SELECT privileges on all future tables in audit schema
ALTER DEFAULT PRIVILEGES IN SCHEMA audit GRANT SELECT ON TABLES TO pine_readonly;

-- Grant usage on all sequences (needed for SELECT queries that might reference sequences)
GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO pine_readonly;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA audit TO pine_readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE ON SEQUENCES TO pine_readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA audit GRANT USAGE ON SEQUENCES TO pine_readonly;

-- Ensure the readonly user cannot perform any write operations
-- (This is implicit with only SELECT grants, but being explicit for clarity)
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA public FROM pine_readonly;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA audit FROM pine_readonly; 