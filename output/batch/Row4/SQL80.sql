SELECT
    id,
    created_at,
    created_user,
    updated_at,
    updated_user,
    deleted,
    display_template,
    memo,
    position,
    publish_country,
    template_type,
    title
FROM
    offerwall_production.templates
WHERE
    deleted = 0;