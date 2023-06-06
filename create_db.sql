-- Create the users schema
CREATE SCHEMA IF NOT EXISTS users;

-- Create the users_revenue table under the users schema
CREATE TABLE users.users_revenue (
  userId text PRIMARY KEY,
  revenue numeric
);