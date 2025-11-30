CREATE ROLE IF NOT EXISTS analyst_full;
CREATE ROLE IF NOT EXISTS analyst_limited;

CREATE USER IF NOT EXISTS user_full IDENTIFIED WITH plaintext_password BY 'user_full';
CREATE USER IF NOT EXISTS user_limited IDENTIFIED WITH plaintext_password BY 'user_limited';

GRANT analyst_full TO user_full;
GRANT analyst_limited TO user_limited;

GRANT SELECT ON views.Voting, views.VotingAnalytics TO analyst_full;
GRANT SELECT ON views.VotingPseudo, views.VotingAnalyticsPseudo TO analyst_limited;