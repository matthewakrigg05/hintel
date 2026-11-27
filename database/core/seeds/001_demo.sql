INSERT OR IGNORE INTO app_user (
    public_id,
    email,
    display_name
) VALUES (
    'demo-user-001',
    'demo@hintel.local',
    'Demo User'
);

INSERT OR IGNORE INTO saved_search (
    user_id,
    name,
    query_json
)
VALUES (
    (SELECT id FROM app_user WHERE public_id = 'demo-user-001'),
    'Manchester family homes',
    '{"county":"Greater Manchester","property_type":"D","bedrooms_max":null}'
);
